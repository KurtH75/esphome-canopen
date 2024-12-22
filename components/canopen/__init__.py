import datetime
from itertools import groupby

import esphome.config_validation as cv
import esphome.codegen as cg
from esphome import automation
from esphome.const import CONF_ID, CONF_TRIGGER_ID
from esphome.components.canbus import CanbusComponent
from esphome.components.mqtt import MQTTClientComponent
from esphome.core import coroutine_with_priority

ns = cg.esphome_ns.namespace('canopen')
CanopenComponent = ns.class_(
    'CanopenComponent',
    cg.Component,
)


CONF_ENTITIES = "entities"

DEPENDENCIES = []

ENTITY_SCHEMA = cv.Schema({
    cv.Required("id"): cv.use_id(cg.EntityBase),
    cv.Required("index"): cv.int_,
    cv.Optional("size"): cv.int_,
    cv.Optional("min_value"): cv.float_,
    cv.Optional("max_value"): cv.float_,
    cv.Optional("tpdo"): cv.int_,
    cv.Optional("rpdo"): cv.ensure_list(RPDO_SCHEMA),
})


TEMPLATE_ENTITY = cv.Schema({
    cv.Required("index"): cv.int_,
    cv.Optional("tpdo", -1): cv.int_,
    cv.Optional("commands"): cv.ensure_list(TEMPLATE_ENTITY_CMD_SCHEMA),
    cv.Optional("states"): cv.ensure_list(TEMPLATE_ENTITY_STATE_SCHEMA),
    cv.Optional("metadata"): TEMPLATE_ENTITY_METADATA_SCHEMA,
})


CONFIG_SCHEMA = cv.Schema({
    cv.GenerateID(): cv.declare_id(CanopenComponent),
    cv.Optional("canbus_id"): cv.use_id(CanbusComponent),
    # cv.GenerateID("ota_id"): cv.use_id(CanopenOTAComponent),
    cv.Optional("mqtt_id"): cv.use_id(MQTTClientComponent),
    cv.Required("node_id"): cv.int_,
    # cv.Optional("status"): STATUS_ENTITY_SCHEMA,
    cv.Optional("csdo"): cv.ensure_list(CSDO_SCHEMA),
    cv.Required(CONF_ENTITIES): cv.ensure_list(ENTITY_SCHEMA),
    cv.Optional("template_entities"): cv.ensure_list(TEMPLATE_ENTITY),
}).extend(cv.COMPONENT_SCHEMA)


def to_code(config):

    node_id = config["node_id"]
    canopen = cg.new_Pvariable(config[CONF_ID], node_id)

    if "canbus_id" in config:
        cg.add_define("USE_CANBUS")
        canbus = yield cg.get_variable(config["canbus_id"])
        cg.add(canopen.set_canbus(canbus))

    yield cg.register_component(canopen, config)

    entities = sorted(config.get(CONF_ENTITIES, []), key=lambda x: x['index'])
    assert len(entities) == len(set(e['index'] for e in entities)), "All entity indices must be unique!"
    for entity_config in entities:
        entity = yield cg.get_variable(entity_config["id"])
        tpdo = entity_config.get("tpdo", -1)
        size = entity_config.get("size")
        if size in (1, 2):
            min_val = entity_config.get("min_value", 0)
            max_val = entity_config.get("max_value", 254 if size == 1 else 65534)  # 255 / 65535 reserved for NaN
            cg.add(canopen.add_entity(entity, entity_config["index"], tpdo, size, min_val, max_val))
        else:
            cg.add(canopen.add_entity(entity, entity_config["index"], tpdo))

    for tmpl_entity in config.get("template_entities", []):
        index = tmpl_entity["index"]
        metadata = tmpl_entity.get("metadata")
        if metadata:
            cg.add(canopen.od_add_metadata(index, metadata["type"], metadata["name"], metadata["device_class"], metadata["unit"], metadata["state_class"]))
        for state in tmpl_entity.get("states", ()):
            _type, size = TYPE_TO_CANOPEN_TYPE[state["type"]]
            cg.add(canopen.od_add_state(index, _type, 0, size, tmpl_entity["tpdo"]))
            pass
        for cmd in tmpl_entity.get("commands", ()):
            for handler in cmd["handler"]:
                trigger = cg.new_Pvariable(handler[CONF_TRIGGER_ID])
                yield automation.build_automation(trigger, [(getattr(cg, cmd["type"]), "x"),], handler)
                cg.add(canopen.add_entity_cmd(index, tmpl_entity.get("tpdo", -1), trigger))

    for num, csdo in enumerate(config.get("csdo", ())):
        cg.add(canopen.setup_csdo(num, csdo["node_id"], csdo["tx_id"], csdo["rx_id"]))
    for num, client in enumerate(config.get("heartbeat_clients", ()), 1):
        cg.add(canopen.setup_heartbeat_client(num, client["node_id"], client["timeout"]))

    for conf in config.get("on_operational", []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID])
        yield automation.build_automation(trigger, [], conf)
        cg.add(canopen.add_trigger(trigger))

    for conf in config.get("on_pre_operational", []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID])
        yield automation.build_automation(trigger, [], conf)
        cg.add(canopen.add_trigger(trigger))

    for conf in config.get("on_hb_consumer_event", []):
        trigger = cg.new_Pvariable(conf[CONF_TRIGGER_ID])
        yield automation.build_automation(trigger, [(cg.uint8, "node_id"),], conf)
        cg.add(canopen.add_trigger(trigger))


    rpdo_entities = [
        {**rpdo, "entity_index": entity["index"]}
        for entity in entities
        for rpdo in entity.get("rpdo", ())
    ]
    rpdo_entities.sort(key=lambda rpdo: (rpdo["node_id"], rpdo["tpdo"], rpdo["offset"]))
    for idx, ((node_id, tpdo), rpdos) in enumerate(
        groupby(rpdo_entities, key=lambda rpdo: (rpdo["node_id"], rpdo["tpdo"]))
    ):
        cg.add(canopen.add_rpdo_node(idx, node_id, tpdo))
        rpdos = list(rpdos)
        curr_offs = 0
        for rpdo in rpdos:
            assert rpdo["offset"] >= curr_offs, f"RPDO: invalid TPDO offset {rpdo}"
            if rpdo["offset"] > curr_offs:
                cg.add(canopen.add_rpdo_dummy(idx, rpdo["offset"] - curr_offs))
                curr_offs = rpdo["offset"]
            cg.add(canopen.add_rpdo_entity_cmd(idx, rpdo["entity_index"], rpdo["cmd"]))
            curr_offs += 1
