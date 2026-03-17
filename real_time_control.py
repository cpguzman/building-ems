import time
from typing import Sequence, Generator
from random import randrange
import datetime
from dataclasses import dataclass
import configparser
import logging
import json
from json import JSONDecodeError

import pendulum
import pymysql

from util.AttrDict import AttrDict
from util.messaging import Messaging, Will
from json_watcher import JsonWatcher

from db.measurements import measurements_open, measurements_get_last, measurements_close
from db.scheduling import scheduling_open, scheduling_next_state, scheduling_get_current, scheduling_close, scheduling_delete
from db.setpoint import setpoint_insert, setpoint_open, setpoint_get_current

# Adding imports to use the API:
from db.api.site.corporate_api import CorporateAPI
from db.api.connection import ConnectionConfig
from db.member.device import DeviceType
from db.enums import DeviceState

#simple_messenger = None

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class MeasurementRto:
    timestamp: str
    device_id: int
    device_type: str
    metric: str
    value: float

def get_config(section: str = 'DEFAULT') -> configparser.SectionProxy:
    """ Get Config """
    config_parser = configparser.ConfigParser()
    config_parser.read('config.ini')
    return config_parser[section]


class ControlLogic:
    def __init__(self, _corporate, name, config, setpoint_file, watcher, scheduling_path):
        self._corporate = _corporate
        self.name = name
        self.config = config
        self.setpoint_file = setpoint_file
        self.watcher = watcher
        self.scheduling_path = scheduling_path

        self.handle = self.open_setpoints()

        self.messenger = create_simple_messenger(self._corporate.id, name, config)
        self._connectors = [
            device for device in self._corporate.devices
            if device.device_type == DeviceType.CONNECTOR
        ]

        self._load = next(
            (
                device
                for device in self._corporate.devices
                if device.device_type == DeviceType.METER
                ),
            None,
        )
        self.setpoints: dict[int, float] = {connector.id: 0 for connector in self._connectors}
        self.connectors_in_app_data: list[int] = []
        self.connectors_in_use: list[int] = []

        # In future, each company could have its owm role list
        self.roleList = ['service', 'visitor', 'employee', 'fleet'] # From the lowest to the highest priority: 0 1 2 3
        self.watcher.on_deleted(self.erase_scheduling)

    def open_setpoints(self):
        """ Open Setpoints """
        while True:
            try:
                return setpoint_open(self.setpoint_file)
            except pymysql.err.OperationalError as e:
                logger.error("Error connecting to setpoints database: %s Retrying in 5 seconds...", e)
                time.sleep(5)

    def round_time(self, timestamp):
        """
        Rounds the time down to the nearest multiple of 15.
        """
        minutes = timestamp.minute
        rounded_minutes = minutes - (minutes % 15)
        return timestamp.replace(minute=rounded_minutes, second=0, microsecond=0)

    def difference_in_multiples_of_x(self, time1, time2, x):
        """
        Calculates the difference between two times in multiples of 15 minutes.
        """
        rounded_time1 = self.round_time(time1)
        rounded_time2 = self.round_time(time2)
        difference = rounded_time2 - rounded_time1
        minutes = difference.total_seconds() / 60
        return int(minutes / x)

    def read_evs_data(self, app_data):
        bat_max_capacity_filtered = []
        for connector_id in self.connectors_in_app_data:
            if connector_id in self.connectors_in_app_data:
                tuple_ = (app_data[connector_id].batMaxCapacity, app_data[connector_id].id)
                bat_max_capacity_filtered.append(tuple_)
        return bat_max_capacity_filtered

    def calculate_priority(self, app_data):
        dict_of_priorities = {}
        target = []
        for connector in self._connectors:
            if connector.id in app_data:
                stepDepartureDateTime = self.difference_in_multiples_of_x(pendulum.now().in_timezone('UTC'),
                                                                        pendulum.parse(app_data[connector.id].departureDateTime),
                                                                        x=15)
                # In case of pendulum.now() = departure_time
                if stepDepartureDateTime <= 0:
                    stepDepartureDateTime = 1
                priority = self.roleList.index(app_data[connector.id].userType.lower()) + 1 # index + 1 in order to not multiplicate with 0.

                dict_of_priorities[app_data[connector.id].id] = priority * (app_data[connector.id].batMaxCapacity -
                                                    app_data[connector.id].batMaxCapacity *
                                                    app_data[connector.id].initialSOC) / stepDepartureDateTime
                tuple_ = (app_data[connector.id].targetSOC, connector.id)
                target.append(tuple_)
        return dict_of_priorities, target

    def erase_scheduling(self, connector_id: int):
        scheduling_handle = scheduling_open(self.scheduling_path)
        next_state_discharging = scheduling_next_state(scheduling_handle, DeviceState.EXPORT.value, connector_id)
        logger.debug("next_state_discharging = %s", next_state_discharging)
        if not next_state_discharging:
            logger.info("Measured SoC is higher than target SoC and there is no discharging. We got on our objective!")
            scheduling_delete(scheduling_handle, connector_id)
            scheduling_close(scheduling_handle)
        else:
            logger.info("There will be discharge at %s. By now, let's send setpoint 0 A to EV and wait for discharging.", next_state_discharging)
        try:
            (self.messenger.publish(
                f'ev4eu/site/edge/device/{connector_id}/setpoint/current', 0)
                .wait_for_publish(5))
        except:
            self.messenger = create_simple_messenger(self._corporate.id, self.name, self.config)

    #Control Logic RTO Function:
    def control_logic_rto(self, data):
        for k, v in self.setpoints.items():
            self.setpoints[k] = 0

        measurement_data = list(data.measurements)
        #logger.debug("measurement_data = %s", measurement_data)

        #Defining the MEASUREMENT variables before the loop (prefix m).
        m_power_active_import = m_power_active_import_l1 = m_power_active_import_l2 = m_power_active_import_l3 = None
        m_power_active_export = m_power_active_export_l1 = m_power_active_export_l2 = m_power_active_export_l3 = None

        # Since there is more than one car, it will be a list.
        m_ev_state_operation = []
        m_current_import_ev = []
        m_current_export_ev = []
        m_ev_state_connection = []
        m_soc = []

        app_data = self.watcher.get_data()

        if not app_data:
            logger.error("There is no information from app data about EVS")
            for connector_, setpoint_ in self.setpoints.items():
                logger.info("CSV|setpoint|%s;%d;%s", pendulum.now('UTC').to_datetime_string(), connector_, setpoint_)
            return 0

        #Filtering the data from what is sent by rto.py about MEASUREMENTS:
        connector_backend_id = -1
        for i, _dict in enumerate(measurement_data):
            if _dict.device_type == "cp":
                if _dict.metric == "power_active_import":
                    m_power_active_import = float(_dict.value)
                    continue
                if _dict.metric == "power_active_import_l1":
                    m_power_active_import_l1 = float(_dict.value)
                    continue
                if _dict.metric == "power_active_import_l2":
                    m_power_active_import_l2 = float(_dict.value)
                    continue
                if _dict.metric == "power_active_import_l3":
                    m_power_active_import_l3 = float(_dict.value)
                    continue
                if _dict.metric == "power_active_export":
                    m_power_active_export = float(_dict.value)
                    continue
                if _dict.metric == "power_active_export_l1":
                    m_power_active_export_l1 = float(_dict.value)
                    continue
                if _dict.metric == "power_active_export_l2":
                    m_power_active_export_l2 = float(_dict.value)
                    continue
                if _dict.metric == "power_active_export_l3":
                    m_power_active_export_l3 = float(_dict.value)
                    continue
            if _dict.device_type == "connector":
                for connector in self._connectors:
                    if int(connector.id)== int(_dict.device_id):
                        connector_backend_id = connector.id # ID that comes from backend API
                        break
                if _dict.metric == "current_import_l1":
                    m_current_import_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "current_import_l2":
                    m_current_import_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "current_import_l3":
                    m_current_import_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "current_export_l1":
                    m_current_export_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "current_export_l2":
                    m_current_export_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "current_export_l3":
                    m_current_export_ev.append((float(_dict.value), connector_backend_id)) #It can be positive or negative.
                    continue
                if _dict.metric == "ev_state_operation":
                    m_ev_state_operation.append((_dict.value, connector_backend_id))
                    continue
                if _dict.metric == "ev_state_connection":
                    m_ev_state_connection.append((_dict.value, connector_backend_id))
                    continue
                if _dict.metric == "soc":
                    m_soc.append((float(_dict.value), int(_dict.device_id)))
                    continue

        logger.debug("m_power_active_import = %s", m_power_active_import)
        logger.debug("m_power_active_import_l1 = %s", m_power_active_import_l1)
        logger.debug("m_power_active_import_l2 = %s", m_power_active_import_l2)
        logger.debug("m_power_active_import_l3 = %s", m_power_active_import_l3)
        logger.debug("m_power_active_export = %s", m_power_active_export)
        logger.debug("m_power_active_export_l1 = %s", m_power_active_export_l1)
        logger.debug("m_power_active_export_l2 = %s", m_power_active_export_l2)
        logger.debug("m_power_active_export_l3 = %s", m_power_active_export_l3)
        logger.debug("m_current_import_ev = %s", m_current_import_ev)
        logger.debug("m_current_export_ev = %s", m_current_export_ev)
        logger.debug("m_ev_state_operation = %s", m_ev_state_operation)
        logger.debug("m_soc = %s", m_soc)
        logger.debug("m_ev_state_connection = %s", m_ev_state_connection)

        power_active_l1 = (
            m_power_active_import_l1 if m_power_active_import_l1 is not None
            else (-m_power_active_export_l1 if m_power_active_export_l1 is not None else None)
        )

        power_active_l2 = (
            m_power_active_import_l2 if m_power_active_import_l2 is not None
            else (-m_power_active_export_l2 if m_power_active_export_l2 is not None else None)
        )

        power_active_l3 = (
            m_power_active_import_l3 if m_power_active_import_l3 is not None
            else (-m_power_active_export_l3 if m_power_active_export_l3 is not None else None)
        )

        logger.debug("power_active_l1 = %s", power_active_l1)
        logger.debug("power_active_l2 = %s", power_active_l2)
        logger.debug("power_active_l3 = %s", power_active_l3)

        #---------------------------------------- DEFINING PRIORITIES ----------------------------------
        dict_of_priorities, target = self.calculate_priority(app_data)
        logger.debug("dict_of_priorities = %s", dict_of_priorities)

        sorted_keys_dict: dict = sorted(dict_of_priorities, key=dict_of_priorities.get, reverse=True)
        logger.debug("sorted_keys_dict = %s", sorted_keys_dict)

        self.connectors_in_app_data = [connector_id for connector_id in dict_of_priorities.keys()]
        logger.debug("self.connectors_in_app_data = %s", self.connectors_in_app_data)

        priority_ids: list[int] = [item for item in sorted_keys_dict if item in self.connectors_in_app_data]
        logger.debug("IDS in order of priority = %s", priority_ids)

        #-------------------CHECKING IF THERE IS SCHEDULING DATA, IF NOT, IT WILL BE GENERATED BY OPT-----------------
        if not data.scheduling:
            logger.warning("No scheduling available for site %s.", self._corporate.id)
            for connector_, setpoint_ in self.setpoints.items():
                self.setpoints[connector_] = 0
                logger.info("CSV|setpoint|%s;%d;%s", pendulum.now('UTC').to_datetime_string(), connector_, setpoint_)
            return 0

        #------------------------------------------ SCHEDULING DATA ------------------------------------------------
        # FORMAT: data_cp['schedulings'] => [(...), (...), (...),...]
        # Getting scheduling information:
        logger.debug("data.scheduling = %s", data.scheduling)

        connectors_in_scheduling = [int(data_.cp_id) for data_ in data.scheduling]
        self.connectors_in_use = [item for item in connectors_in_scheduling if item in self.connectors_in_app_data]
        logger.debug("self.connectors_in_use before sorting = %s", self.connectors_in_use)
        self.connectors_in_use = [cid for cid in priority_ids if cid in self.connectors_in_use]
        logger.debug("self.connectors_in_use AFTER sorting = %s", self.connectors_in_use)

        #---------------------------------- MEASUREMENT DATA SORTED -----------------------------------
        m_soc_filtered: list[tuple] = [tuple_ for tuple_ in m_soc if tuple_[1] in self.connectors_in_use]
        m_soc_sorted: list[float] = [x[0] for x in sorted(m_soc_filtered, key=lambda x: priority_ids.index(x[1]))]

        logger.debug("m_soc_sorted = %s", m_soc_sorted)

        #------------------------------------------ STATIC DATA ------------------------------------------------
        # EVS:
        target_filtered = [tuple_ for tuple_ in target if tuple_[1] in self.connectors_in_use]
        target_sorted: list[float] = [x[0] for x in sorted(target_filtered, key=lambda x: priority_ids.index(x[1]))]

        logger.debug("target_sorted = %s", target_sorted)
        # Connectors:
        st_conn_min_charge_power_filtered: list[tuple] = [(connector.min_power_charge_rate, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
        logger.debug("st_conn_min_charge_power_filtered = %s", st_conn_min_charge_power_filtered)
        st_conn_min_charge_power_sorted: list[float] = [x[0] for x in sorted(st_conn_min_charge_power_filtered, key=lambda x: priority_ids.index(x[1]))]

        st_conn_max_charge_power_filtered: list[tuple] = [(connector.max_power_charge_rate, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
        st_conn_max_charge_power_sorted: list[float] = [x[0] for x in sorted(st_conn_max_charge_power_filtered, key=lambda x: priority_ids.index(x[1]))]

        st_conn_max_discharge_power_filtered: list[tuple] = [(connector.max_power_discharge_rate, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
        st_conn_max_discharge_power_sorted: list[float] = [x[0] for x in sorted(st_conn_max_discharge_power_filtered, key=lambda x: priority_ids.index(x[1]))]

        st_conn_phase_filtered: list[tuple] = [(connector.connected_phase, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
        st_conn_phase_sorted: list[bool] = [x[0] for x in sorted(st_conn_phase_filtered, key=lambda x: priority_ids.index(x[1]))]

        st_conn_is_v2g_filtered: list[tuple] = [(connector.v2g_available, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
        st_conn_is_v2g_sorted: list[bool] = [x[0] for x in sorted(st_conn_is_v2g_filtered, key=lambda x: priority_ids.index(x[1]))]

        # Installation:
        st_site_power_contracted: float = self._corporate.installed_power

        logger.debug("st_site_power_contracted = %s", st_site_power_contracted)
        logger.debug("st_conn_max_charge_power_sorted = %s", st_conn_max_charge_power_sorted)
        logger.debug("st_conn_min_charge_power_sorted = %s", st_conn_min_charge_power_sorted)
        logger.debug("st_conn_max_discharge_power_sorted = %s", st_conn_max_discharge_power_sorted)
        logger.debug("st_conn_phase_sorted = %s", st_conn_phase_sorted)
        logger.debug("st_conn_is_v2g_sorted = %s", st_conn_is_v2g_sorted)

        bat_max_capacity_filtered: list[tuple] = self.read_evs_data(app_data)
        bat_max_capacity_sorted: list[float] = [x[0] for x in sorted(bat_max_capacity_filtered, key=lambda x: priority_ids.index(x[1]))]
        logger.debug("bat_max_capacity_sorted = %s", bat_max_capacity_sorted)

        #------------------------------------------ SCHEDULING DATA ------------------------------------------------
        sch_time_string_filtered: list[tuple] = [(data_.ts, int(data_.cp_id)) for data_ in data.scheduling if int(data_.cp_id) in self.connectors_in_use]
        sch_time_string_sorted: list[str] = [x[0] for x in sorted(sch_time_string_filtered, key=lambda x: priority_ids.index(x[1]))]

        sch_ev_power_filtered: list[tuple] = [(data_.ev_power, int(data_.cp_id)) for data_ in data.scheduling if int(data_.cp_id) in self.connectors_in_use]
        sch_ev_power_sorted: list[float] = [x[0] for x in sorted(sch_ev_power_filtered, key=lambda x: priority_ids.index(x[1]))]

        sch_ev_state_filtered: list[tuple] = [(data_.ev_state, int(data_.cp_id)) for data_ in data.scheduling if int(data_.cp_id) in self.connectors_in_use]
        sch_ev_state_sorted: list[float] = [float(x[0]) for x in sorted(sch_ev_state_filtered, key=lambda x: priority_ids.index(x[1]))]

        sch_data_site_state_filtered: list[tuple] = [(data_.site_state, int(data_.cp_id)) for data_ in data.scheduling if int(data_.cp_id) in self.connectors_in_use]
        sch_data_site_state_sorted: list[float] = [float(x[0]) for x in sorted(sch_data_site_state_filtered, key=lambda x: priority_ids.index(x[1]))]

        sch_ev_energy_filtered: list[tuple] = [(data_.ev_soc, int(data_.cp_id)) for data_ in data.scheduling if int(data_.cp_id) in self.connectors_in_use]
        sch_ev_energy_sorted: list[float] = [x[0] for x in sorted(sch_ev_energy_filtered, key=lambda x: priority_ids.index(x[1]))]

        sch_ev_soc_sorted: list[float] = [sch_ev_energy_sorted[i] / bat_max_capacity_sorted[j]
                                          for i in range(len(sch_ev_energy_sorted))
                                          for j in range(len(bat_max_capacity_sorted))
                                          if i == j]

        logger.debug("sch_ev_power_sorted = %s", sch_ev_power_sorted)
        logger.debug("sch_ev_state_sorted = %s", sch_ev_state_sorted)
        logger.debug("sch_data_site_state_sorted = %s", sch_data_site_state_sorted)
        logger.debug("sch_time_string_sorted = %s", sch_time_string_sorted)
        logger.debug("sch_ev_energy_sorted = %s", sch_ev_energy_sorted)
        logger.debug("sch_ev_soc_sorted = %s", sch_ev_soc_sorted)

        #The max current of grid taking into account that the site is three phase.
        st_i_grid_max: float = st_site_power_contracted / 690

        # Getting information from connectors, already sorted according to priority.
        i_sch_ev: list[float]  = [
            sch_ev_power_sorted[i] / 230
            for i in range(len(sch_ev_power_sorted))
        ]
        logger.debug("i_sch_ev = %s", i_sch_ev)

        st_i_conn_max: list[float] =  [
            st_conn_max_charge_power_sorted[i] / 230
            for i in range(len(st_conn_max_charge_power_sorted))
        ]
        logger.debug("st_i_conn_max = %s", st_i_conn_max)

        st_i_conn_min: list[float] =  [
            st_conn_min_charge_power_sorted[i] / 230
            for i in range(len(st_conn_min_charge_power_sorted))
        ]
        logger.debug("st_i_conn_min = %s", st_i_conn_min)

        while True:
            try:
                i_ev_ant = setpoint_get_current(self.handle)
                logger.debug("Just find the i_ev_ant in setpoint db. %s", i_ev_ant)
                break
            except pymysql.err.OperationalError as e:
                logger.warning("Issues getting current setpoint: %s", e)
                time.sleep(5)
                self.handle = self.open_setpoints()

        if not i_ev_ant:
            i_ev_ant: list[tuple] = [(0, connector.id) for connector in self._connectors if connector.id in self.connectors_in_use]
            logger.debug("No previous setpoint info")
        else:
            i_ev_ant: list[tuple] = [(float(tuple_[2]), float(tuple_[1])) for tuple_ in i_ev_ant if int(float(tuple_[1])) in self.connectors_in_use] #(setpoint, conn_id)
            logger.debug("Previous setpoint info: %s", i_ev_ant)

        # Sorting according to the priorities
        i_ev_ant_sorted: list[float] = [x[0] for x in sorted(i_ev_ant, key=lambda x: priority_ids.index(x[1]))]
        logger.debug("i_ev_ant after sorting: %s", i_ev_ant_sorted)

        m_igrid_ph1 = (power_active_l1 / 230) if power_active_l1 is not None else None
        m_igrid_ph2 = (power_active_l2 / 230) if power_active_l2 is not None else None
        m_igrid_ph3 = (power_active_l3 / 230) if power_active_l3 is not None else None
        logger.debug("m_igrid_ph1 = %s", m_igrid_ph1)
        logger.debug("m_igrid_ph2 = %s", m_igrid_ph2)
        logger.debug("m_igrid_ph3 = %s", m_igrid_ph3)

        i_ch_available_ph1 = (st_i_grid_max - m_igrid_ph1) if m_igrid_ph1 is not None else None
        i_ch_available_ph2 = (st_i_grid_max - m_igrid_ph2) if m_igrid_ph2 is not None else None
        i_ch_available_ph3 = (st_i_grid_max - m_igrid_ph3) if m_igrid_ph3 is not None else None

        logger.debug("st_i_grid_max = %s", st_i_grid_max)
        logger.debug("i_ch_available_ph1 = %s", i_ch_available_ph1)
        logger.debug("i_ch_available_ph2 = %s", i_ch_available_ph2)
        logger.debug("i_ch_available_ph3 = %s", i_ch_available_ph3)

        if i_ch_available_ph1 is None:
            i_ch_aux_ph1 = None
        else:
            i_ch_aux_ph1 = 0
            for i, value_ in enumerate(i_ev_ant_sorted):
                if st_conn_phase_sorted[i] == 1:
                    i_ch_aux_ph1 += i_ch_available_ph1 + value_  # i_ev_ant high, i_ch_available low and vice-versa.

        if i_ch_available_ph2 is None:
            i_ch_aux_ph2 = None
        else:
            i_ch_aux_ph2 = 0
            for i, value_ in enumerate(i_ev_ant_sorted):
                if st_conn_phase_sorted[i] == 2:
                    i_ch_aux_ph2 += i_ch_available_ph2 + value_  # i_ev_ant high, i_ch_available low and vice-versa.

        if i_ch_available_ph3 is None:
            i_ch_aux_ph3 = None
        else:
            i_ch_aux_ph3 = 0
            for i, value_ in enumerate(i_ev_ant_sorted):
                if st_conn_phase_sorted[i] == 3:
                    i_ch_aux_ph3 += i_ch_available_ph3 + value_  # i_ev_ant high, i_ch_available low and vice-versa.

        i_ch_aux = m_igrid = None

        #------------------------------------------ BEGINNING OF CALCULATIONS ------------------------------------------------
        for i, connector_id in enumerate(self.connectors_in_use):
            setpoint_ev_current = 0
            phase_ = st_conn_phase_sorted[i]
            logger.debug("Connector %s is on phase %s", connector_id, phase_)
            if phase_ == 1:
                i_ch_aux = i_ch_aux_ph1
                m_igrid = m_igrid_ph1
            elif phase_ == 2:
                i_ch_aux = i_ch_aux_ph2
                m_igrid = m_igrid_ph2
            elif phase_ == 3:
                i_ch_aux = i_ch_aux_ph3
                m_igrid = m_igrid_ph3
            else:
                logger.error("Connector %s has no phase specification in backend.", connector_id)
            if i_ch_aux is None or m_igrid is None:
                logger.error("Connector %s has no valid i_ch_aux %s or m_igrid %s value.", connector_id, i_ch_aux, m_igrid)
                continue

            if i_sch_ev[i] != 0:
                if sch_ev_state_sorted[i] == DeviceState.IMPORT: #EV charging
                    # This array has all the minimum charge current accept by connectors, but it won't consider if scheduling for that car is 0 A.
                    for k, item in enumerate(st_i_conn_min):
                        if i_sch_ev[k] == 0 or phase_ != st_conn_phase_sorted[k]:
                            st_i_conn_min[k] = 0
                    if sch_data_site_state_sorted == DeviceState.EXPORT:
                        if i_ch_aux >= st_i_conn_min[i]:
                            if m_igrid <= 0:
                                setpoint_ev_current = min(i_sch_ev[i], i_ch_aux)
                            else:
                                if i_ch_aux - sum(st_i_conn_min[i+1:]) >= st_i_conn_min[i]: # If it is possible to charge all cars as planned, at least with the minimum...
                                    setpoint_ev_current = min(i_ch_aux - sum(st_i_conn_min[i+1:]), i_sch_ev[i])
                                else: # To not waste energy in the case is not possible to charge all cars as we have planned, we send all to the car with most priority.
                                    setpoint_ev_current = min(i_ch_aux, i_sch_ev[i])
                        else:
                            logger.warning("There is no enough available power to charge the car.")
                    else: # site_state = import
                        if i_ch_aux >= st_i_conn_min[i]:
                            if i_ch_aux - sum(st_i_conn_min[i+1:]) >= st_i_conn_min[i]: # If it is possible to charge all cars as planned, at least with the minimum...
                                if m_igrid >= 0:
                                    setpoint_ev_current = min(i_ch_aux - sum(st_i_conn_min[i+1:]), i_sch_ev[i])
                                else:
                                    setpoint_ev_current = min(i_ch_aux - sum(st_i_conn_min[i+1:]), i_sch_ev[i]) + abs(m_igrid / len(self.connectors_in_use)) # If the grid is negative, it's possible to slightly increase the value of sch: i_sch_ev + m_igrid
                            else: # To not waste energy in the case is not possible to charge all cars as we have planned, we send all to the car with most priority.
                                if m_igrid >= 0:
                                    setpoint_ev_current = min(i_ch_aux, i_sch_ev[i])
                                else:
                                    # If the grid is negative, it's possible to slightly increase the value of sch: i_sch_ev + m_igrid
                                    setpoint_ev_current = min(i_ch_aux, i_sch_ev[i]) + abs(m_igrid / len(self.connectors_in_use))
                        else:
                            logger.warning("There is no enough available power to charge the car.")
                elif sch_ev_state_sorted[i] == DeviceState.EXPORT: #EV discharging
                    setpoint_ev_current = - i_sch_ev[i]
                    logger.info("sch state for car %s is discharging! Setpoint current is negative %s", self.connectors_in_use[i], setpoint_ev_current)
            else:
                logger.info("The i_sch_ev is %s. The car won't charge now, it is needed to wait.", i_sch_ev)

            flag = 0
            if st_conn_is_v2g_sorted[i] == True and m_soc_sorted and len(m_soc_sorted) == len(sch_ev_soc_sorted):
                logger.debug("The connector is V2G! Let's see how we can use this.")
                if DeviceState.IMPORT in sch_ev_state_sorted and sch_ev_state_sorted[i] != DeviceState.IMPORT: # If any other EV is planned to charge
                    logger.info("There is a EV charging. Maybe we can use the V2G EV to help the other EV to charge.")
                    if (m_soc_sorted[i] + 0.02) > sch_ev_soc_sorted[i] and sch_ev_state_sorted[i] != DeviceState.EXPORT:
                        scheduling_handle = scheduling_open(self.scheduling_path)
                        next_state_charging = scheduling_next_state(scheduling_handle, DeviceState.IMPORT.value, connector_id)
                        logger.debug("next_state_charging = %s", next_state_charging)
                        if not next_state_charging:
                            logger.info("There is no charging planned to v2g EV. So, we are not going to discharge the EV.")
                        else:
                            logger.debug("i_sch_ev = %s", i_sch_ev)
                            max_i_sch_ev = max(i_sch_ev)
                            logger.debug("max_i_sch_ev = %s", max_i_sch_ev)
                            setpoint_ev_current = - max_i_sch_ev
                            flag = 1

            if m_soc_sorted and flag == 0 and len(m_soc_sorted) == len(sch_ev_soc_sorted):
                scheduling_handle = scheduling_open(self.scheduling_path)
                next_state_charging = scheduling_next_state(scheduling_handle, DeviceState.IMPORT.value, connector_id)
                logger.debug("next_state_charging = %s", next_state_charging)
                if m_soc_sorted[i] >= target_sorted[i] and sch_ev_state_sorted[i] != DeviceState.EXPORT:
                    logger.info("The EV SOC %s is already higher than target SOC %s. Let's see if there is discharging planned.", m_soc_sorted[i], target_sorted[i])
                    self.erase_scheduling(connector_id)
                    setpoint_ev_current = 0
                elif m_soc_sorted[i] < target_sorted[i] and not next_state_charging:
                    logger.info("The EV SOC %s is lower than target SOC %s and there is no charge predict. Let's charge EV.", m_soc_sorted[i], target_sorted[i])
                    setpoint_ev_current = st_i_conn_min[i]
                    logger.info("st_i_conn_min = %s, i = %s", st_i_conn_min, i)
                    logger.info("Let's Charge with setpoint_ev_current = %s.", setpoint_ev_current)

            #------------------------------------- ROUNDING SETPOINT -----------------------------------------
            if setpoint_ev_current >= 0:
                setpoint_ev_current = min(setpoint_ev_current, st_i_conn_max[i])
                aux = round(setpoint_ev_current)
                if (setpoint_ev_current - aux) >= 0:                  #Ex: 3.4 - 3 >= 0
                    setpoint_ev_current = round(setpoint_ev_current)
                else:                                              #Ex: 3.7 - 4 < 0
                    setpoint_ev_current = round(setpoint_ev_current) - 1
            else:
                setpoint_ev_current = max(setpoint_ev_current, -st_i_conn_max[i])
                aux = round(setpoint_ev_current)
                if (setpoint_ev_current - aux) <= 0:                  #Ex: - 6.03 - (-6) <= 0
                    setpoint_ev_current = round(setpoint_ev_current)
                else:                                              #Ex: - 6.96 - (-7) > 0
                    setpoint_ev_current = round(setpoint_ev_current) - 1

            if phase_ == 1:
                i_ch_aux_ph1 -= setpoint_ev_current
                m_igrid_ph1 += setpoint_ev_current
            elif phase_ == 2:
                i_ch_aux_ph2 -= setpoint_ev_current
                m_igrid_ph2 += setpoint_ev_current
            elif phase_ == 3:
                i_ch_aux_ph3 -= setpoint_ev_current
                m_igrid_ph3 += setpoint_ev_current

            self.setpoints[connector_id] = setpoint_ev_current

            logger.info("Loop %s: i_ch_aux = %s, setpoint_ev_current = %s", i, i_ch_aux, setpoint_ev_current)

        for connector_id, value in self.setpoints.items():
            logger.info("CSV|setpoint|%s;%d;%s", pendulum.now('UTC').to_datetime_string(), connector_id, value)
        logger.info("The calculated setpoints are %s.", self.setpoints)

    def publish_setpoint(self):
        #Publishing the setpoint on ev4eu topic:
        for connector_id, current in self.setpoints.items():
            value = current if current is not None else 0
            try:
                (self.messenger.publish(
                    f'ev4eu/site/{self._corporate.id}/device/{connector_id}/setpoint/current', value)
                    .wait_for_publish(5))
            except:
                self.messenger = create_simple_messenger(self._corporate.id, self.name, self.config)

            # Appending on setpoint database:
            current_time = pendulum.now('UTC').to_datetime_string()
            while True:
                try:
                    setpoint_insert(self.handle, current_time, connector_id, value, "current")
                    logger.info("Just insert %s A on setpoint db for connector %s.", value, connector_id)
                    break
                except pymysql.err.OperationalError as e:
                    logger.warning("Issues detected inserting setpoints to database: %s", e)
                    self.handle = self.open_setpoints()
        return 0

def after(date):
    if isinstance(date, str):
        date = pendulum.parse(date)
    # Round down to the nearest multiple of 15 minutes and subtract 15 minutes
    previous_hour = date - datetime.timedelta(minutes=date.minute % 15 + 15)
    previous_hour = previous_hour.replace(second=0)
    return previous_hour.strftime("%Y-%m-%d %H:%M:%S")


def get_data_generator(_storage_configs, site) -> Generator[AttrDict, None, None]:
    # check 6.2.9.1. Generator-iterator methods

    devices_site = [
        device.id for device in site.devices
    ]

    logger.debug("devices_site = %s", devices_site)

    latest = AttrDict({})
    static_data = AttrDict({})
    if _storage_configs.static:
        for static_key, static_file in _storage_configs.static.items():
            try:
                with open(static_file, "r") as json_fd:
                    data = json.load(json_fd)
                    if issubclass(data.__class__, list):
                        static_data[static_key] = [AttrDict(x) for x in data]
                    else:
                        static_data[static_key] = AttrDict(data)
            except JSONDecodeError:
                logger.warning("Error loading json file: %s", static_file)
            except FileNotFoundError:
                logger.error(f"File not found: %s", static_file)

    while True:
        try:
            measurements_dbh = measurements_open(_storage_configs.db.measurements)
            break
        except pymysql.err.OperationalError as e:
            logger.error("Error connecting to measurements database. Retrying in 5 seconds... %s", e)
            time.sleep(5)

    while True:
        try:
            scheduling_dbh = scheduling_open(_storage_configs.db.scheduling)
            break
        except pymysql.err.OperationalError:
            logger.error("Error connecting to scheduling database. Retrying in 5 seconds...")
            time.sleep(5)

    extra_params = {}

    if "metric" in _storage_configs and _storage_configs["metric"] is not None:
        extra_params["metric"] = _storage_configs["metric"]
    if "device" in _storage_configs and _storage_configs["device"] is not None:
        extra_params["device"] = _storage_configs["device"]

    try:
        while True:
            updated = []
            current_time: datetime = pendulum.now("UTC")
            previous_time: str = after(current_time)

            _measurements = []
            _scheduling_data = []

            while True:
                try:
                    measurements_dbh_1 = measurements_open(_storage_configs.db.measurements)
                    _measurements = measurements_get_last(
                        measurements_dbh_1, devices_site, **extra_params, after=previous_time
                    )
                    break
                except pymysql.err.OperationalError:
                    logger.warning("Issues detected getting data from last measurements.")
                    while True:
                        try:
                            measurements_dbh = measurements_open(_storage_configs.db.measurements)
                            break
                        except pymysql.err.OperationalError:
                            logger.error("Error connecting to measurements database. Retrying in 5 seconds...")
                            time.sleep(5)

            # _metrics: Sequence[AttrDict] = [AttrDict({"ts": m[1], "device_id": m[2], "device_type": m[3], "metric": m[4], "value": m[5]}) for m
            #                                in _measurements]

            _metrics: Sequence[AttrDict] = [
                AttrDict(
                    {
                        "ts": m[0],
                        "device_id": m[1],
                        "device_type": m[2],
                        "metric": m[3],
                        "value": m[4],
                    }
                )
                for m in _measurements
            ]

            while True:
                try:
                    scheduling_dbh_1 = scheduling_open(_storage_configs.db.scheduling)
                    _scheduling_data = scheduling_get_current(scheduling_dbh_1, after=previous_time)
                    scheduling_close(scheduling_dbh_1)
                    break
                except pymysql.err.OperationalError:
                    logger.warning("Issues detected getting data from current scheduling.")
                    while True:
                        try:
                            scheduling_dbh = scheduling_open(_storage_configs.db.scheduling)
                            break
                        except pymysql.err.OperationalError:
                            logger.error("Error connecting to scheduling database. Retrying in 5 seconds...")
                            time.sleep(5)


            _scheduling: Sequence[AttrDict] = [
                AttrDict(
                    {
                        "ts": s[0],
                        "cp_id": s[1],
                        "ev_state": s[2],
                        "ev_soc": s[3],
                        "ev_power": s[4],
                        "bess_state": s[5],
                        "bess_soc": s[6],
                        "bess_power": s[7],
                        "site_id": s[8],
                        "site_state": s[9],
                    }
                )
                for s in _scheduling_data
            ]

            for m in _metrics:
                ref = (m.device_id, m.metric)
                if ref not in latest or latest[ref].value != m.value:
                    updated.append(ref)
                    m.updated = True
                latest[ref] = m

            yield AttrDict(
                {
                    "static": static_data,
                    "measurements": list(latest.values()),
                    "scheduling": _scheduling,
                    "scheduling_path": _storage_configs.db.scheduling,
                }
            )

    finally:
        measurements_close(measurements_dbh)
        scheduling_close(scheduling_dbh)

def create_simple_messenger(site_id, name, config):
    """ Create Simple Messenger """
    status_topic = f"ev4eu/daemons/rto/site/{site_id}/{name}/status"
    status_online_message = "online"
    status_offline_message = "offline"

    lwt: Will = Will(status_topic, status_offline_message, retain=True)

    messenger = Messaging(config, will=lwt)

    messenger.publish(status_topic, status_online_message, retain=True)

    return messenger

def main():
    """ main """
    config_common = get_config("common")
    config = get_config("corporate-control-logic")

    logger_level: str = config_common.get("logger_level", "WARNING")
    logger_level: str = config.get("logger_level", logger_level)
    logging.basicConfig(level=logger_level, force=True, format='%(asctime)s [%(name)s] %(levelname)s %(message)s')

    logger.info('Start of main.')

    site_id = int(config_common.get("site_id", "-1"))

    if site_id < 0:
        logger.error("Invalid Site ID: check config.ini file.")


    scheduling_path = config.get('scheduling_db')
    measurements_path = config.get('measurements_db')

    storage_configs = AttrDict({
        'db': {
            'measurements': measurements_path,
            'scheduling': scheduling_path,
        }
    })



    setpoint_file = config.get("setpoint_db")

    name = config.get("name", f"corporate-control-logic-{randrange(100)}")

    db_hostname = config.get("db_hostname")
    db_port = int(config.get("db_port", "3306"))
    db_username = config.get("db_username")
    db_password = config.get("db_password")
    db_database = config.get("db_database")

    # Connect to the database
    connection_config = ConnectionConfig(host=db_hostname,
                                         port=db_port,
                                         user=db_username,
                                         password=db_password,
                                         database=db_database,
                                         charset="utf8mb4")

    api = CorporateAPI(connection_config)
    _corporate = api.get(site_id)

    data = get_data_generator(storage_configs, _corporate)

    watcher = JsonWatcher(site_id, config)

    control_corporate = ControlLogic(_corporate, name, config, setpoint_file, watcher, scheduling_path)

    try:
        while True:
            input_data: AttrDict = next(data)
            control_corporate.control_logic_rto(input_data)
            control_corporate.publish_setpoint()
            time.sleep(15)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()