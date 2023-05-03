#!/usr/bin/env python

# General imports
from argparse import ArgumentParser
from concurrent import futures
from threading import Thread, Event
from socket import AF_INET, AF_INET6
import logging
import time
import grpc
# pymerang dependencies
from pymerang import utils
from pymerang import tunnel_utils
from pymerang import pymerang_pb2
from pymerang import pymerang_pb2_grpc
from pymerang import status_codes_pb2
# SRv6 dependencies
from srv6_sdn_controller_state import srv6_sdn_controller_state
from srv6_sdn_controller_state.srv6_sdn_controller_state import DeviceState

from rollbackcontext import RollbackContext

# Loopback IP address of the controller
DEFAULT_PYMERANG_SERVER_IP = '::'
# Port of the gRPC server executing on the controller
DEFAULT_PYMERANG_SERVER_PORT = 50061
# Default interval between two keep alive messages
DEFAULT_KEEP_ALIVE_INTERVAL = 5
# Max number of keep alive messages lost
# before taking a corrective action
DEFAULT_MAX_KEEP_ALIVE_LOST = 4
# Secure option
DEFAULT_SECURE = False
# Server certificate
DEFAULT_CERTIFICATE = 'cert_server.pem'
# Server key
DEFAULT_KEY = 'key_server.pem'

# Default VXLAN port
DEFAULT_VXLAN_PORT = 4789

# Status codes
STATUS_SUCCESS = status_codes_pb2.STATUS_SUCCESS
STATUS_UNAUTHORIZED = status_codes_pb2.STATUS_UNAUTHORIZED
STATUS_INTERNAL_ERROR = status_codes_pb2.STATUS_INTERNAL_ERROR

MAX_ALLOWED_RECONCILIATION_FAILURES = 10000000


class PymerangServicer(pymerang_pb2_grpc.PymerangServicer):
    """Provides methods that implement functionality of route guide server."""

    def __init__(self, controller):
        self.controller = controller

    def RegisterDevice(self, request, context):
        logging.info('\n\n=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-==-=-=-=New device connected: %s' % request)

        logging.info('=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-==-=-=-=\n\n')

        
        # Get the IP address seen by the gRPC server
        # It can be used for management
        mgmtip = context.peer()
        # Separate IP and port
        mgmtip = utils.parse_ip_port(mgmtip)[0].__str__()
        # Extract the parameters from the registration request
        #
        # Device ID
        deviceid = request.device.id
        # Features supported by the device
        features = list()
        for feature in request.device.features:
            name = feature.name
            port = feature.port
            features.append({'name': name, 'port': port})
        # Data needed for the device authentication
        auth_data = request.auth_data
        # Prefix to be used for SRv6 tunnels
        sid_prefix = None
        if request.sid_prefix != '':
            sid_prefix = request.sid_prefix
        # Define whether to enable or not proxy NDP for SIDs advertisement
        enable_proxy_ndp = request.enable_proxy_ndp
        # Define whether to force the device using ip6tnl or not
        force_ip6tnl = request.force_ip6tnl
        # Define whether to force the device using SRH or not
        force_srh = request.force_srh
        # Incoming Segment Routing transparency [ t0 | t1 | op ]
        incoming_sr_transparency = request.incoming_sr_transparency
        if incoming_sr_transparency == pymerang_pb2.SRTransparency.T1:
            incoming_sr_transparency = 't1'
        elif incoming_sr_transparency == pymerang_pb2.SRTransparency.OP:
            incoming_sr_transparency = 'op'
        else:  # by default we assume T0 transparency
            incoming_sr_transparency = 't0'
        # Outgoing Segment Routing transparency [ t0 | t1 | op ]
        outgoing_sr_transparency = request.outgoing_sr_transparency
        if outgoing_sr_transparency == pymerang_pb2.SRTransparency.T1:
            outgoing_sr_transparency = 't1'
        elif outgoing_sr_transparency == pymerang_pb2.SRTransparency.OP:
            outgoing_sr_transparency = 'op'
        else:  # by default we assume T0 transparency
            outgoing_sr_transparency = 't0'
        # Public prefix length used to compute SRv6 SID list
        public_prefix_length = 128
        if request.public_prefix_length != 0:
            public_prefix_length = request.public_prefix_length
        # Interfaces of the devices
        interfaces = list()
        for interface in request.interfaces:
            # Interface name
            ifname = interface.name
            # MAC address
            mac_addr = interface.mac_addr
            # IPv4 addresses
            ipv4_addrs = list()
            for addr in interface.ipv4_addrs:
                ipv4_addrs.append(addr)     # TODO add validation checks?
            # IPv6 addresses
            ipv6_addrs = list()
            for addr in interface.ipv6_addrs:
                ipv6_addrs.append(addr)     # TODO add validation checks?
            # IPv4 subnets
            ipv4_subnets = list()
            for ipv4_subnet in interface.ipv4_subnets:
                ipv4_subnet.gateway
                subnet = dict()
                subnet['subnet'] = ipv4_subnet.subnet
                if ipv4_subnet.gateway != '':
                    subnet['gateway'] = ipv4_subnet.gateway
                ipv4_subnets.append(subnet)     # TODO add validation checks?
            # IPv6 subnets
            ipv6_subnets = list()
            for ipv6_subnet in interface.ipv6_subnets:
                ipv6_subnet.gateway
                subnet = dict()
                subnet['subnet'] = ipv6_subnet.subnet
                if ipv6_subnet.gateway != '':
                    subnet['gateway'] = ipv6_subnet.gateway
                ipv6_subnets.append(subnet)     # TODO add validation checks?
            # Save the interface
            interfaces.append(
                {
                    'name': ifname,
                    'mac_addr': mac_addr,
                    'ipv4_addrs': ipv4_addrs,
                    'ipv6_addrs': ipv6_addrs,
                    'ipv4_subnets': ipv4_subnets,
                    'ipv6_subnets': ipv6_subnets,
                    'ext_ipv4_addrs': list(),
                    'ext_ipv6_addrs': list(),
                    'type': utils.InterfaceType.UNKNOWN,
                }
            )
        # Prepare the response message
        reply = pymerang_pb2.RegisterDeviceReply()
        # Register the device
        logging.debug('Trying to register the device %s', deviceid)
        response, vxlan_port, tenantid = (
            self.controller.register_device(
                deviceid,
                features,
                interfaces,
                mgmtip,
                auth_data,
                sid_prefix,
                public_prefix_length,
                enable_proxy_ndp,
                force_ip6tnl,
                force_srh,
                incoming_sr_transparency,
                outgoing_sr_transparency
            )
        )
        if response == STATUS_UNAUTHORIZED:
            return (
                pymerang_pb2.RegisterDeviceReply(status=STATUS_UNAUTHORIZED)
            )
        if response != STATUS_SUCCESS:
            # Get the device
            device = srv6_sdn_controller_state.get_device(
                deviceid=deviceid, tenantid=tenantid
            )
            if device is None:
                logging.error('Error getting device')
                return status_codes_pb2.STATUS_INTERNAL_ERROR
            return (
                pymerang_pb2.RegisterDeviceReply(
                    status=response,
                    device_state=device.get('state', DeviceState.UNKNOWN.value)
                )
            )
        # Set the status code
        reply.status = STATUS_SUCCESS
        # Set the VXLAN port
        reply.mgmt_info.vxlan_port = vxlan_port
        # Set the tenant ID
        reply.tenantid = tenantid
        # Get the device
        device = srv6_sdn_controller_state.get_device(
            deviceid=deviceid, tenantid=tenantid
        )
        if device is None:
            logging.error('Error getting device')
            return status_codes_pb2.STATUS_INTERNAL_ERROR
        reply.device_state = device.get('state', DeviceState.UNKNOWN.value)
        # Send the reply
        logging.info(
            'Device registered succefully. Sending the reply: %s', reply
        )
        return reply

    def UpdateMgmtInfo(self, request, context):
        logging.info('*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_Establish tunnel connection: %s', request)
        logging.info('*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*')

        # Get the IP address seen by the gRPC server
        # It can be used for management
        mgmtip = context.peer()
        # Separate IP and port
        mgmtip = utils.parse_ip_port(mgmtip)[0].__str__()
        # Extract the parameters from the registration request
        #
        # Device ID
        deviceid = request.device.id
        # Tenant ID
        tenantid = request.tenantid
        # Interfaces of the devices
        interfaces = dict()
        for interface in request.interfaces:
            # Interface name
            ifname = interface.name
            # IPv4 addresses
            ipv4_addrs = list()
            for addr in interface.ext_ipv4_addrs:
                ipv4_addrs.append(addr)     # TODO add validation checks?
            # IPv6 addresses
            ipv6_addrs = list()
            for addr in interface.ext_ipv6_addrs:
                ipv6_addrs.append(addr)     # TODO add validation checks?
            # Save the interface
            interfaces[ifname] = {
                'name': ifname,
                'ext_ipv4_addrs': ipv4_addrs,
                'ext_ipv6_addrs': ipv6_addrs
            }
        # Extract tunnel mode
        tunnel_mode = request.mgmt_info.tunnel_mode
        # Extract NAT type
        nat_type = request.mgmt_info.nat_type
        # Extract the external IP address
        device_external_ip = request.mgmt_info.device_external_ip
        # Extract the external port
        device_external_port = request.mgmt_info.device_external_port
        # Extract device VTEP MAC address
        device_vtep_mac = request.mgmt_info.device_vtep_mac
        # Extract VXLAN port
        vxlan_port = request.mgmt_info.vxlan_port
        # Update management information
        logging.debug(
            'Trying to update management information for the device %s',
            deviceid
        )
        response, controller_vtep_mac, controller_vtep_ip, device_vtep_ip, \
            vtep_mask = self.controller.update_mgmt_info(
                deviceid, tenantid, interfaces, mgmtip, tunnel_mode, nat_type,
                device_external_ip, device_external_port,
                device_vtep_mac, vxlan_port
            )
        if response != STATUS_SUCCESS:
            logging.error('Cannot update management information')
            return pymerang_pb2.RegisterDeviceReply(status=response)
        # Create the response
        reply = pymerang_pb2.RegisterDeviceReply()
        # Set the status code
        reply.status = STATUS_SUCCESS
        # Set the controller VTEP MAC
        if controller_vtep_mac is not None:
            reply.mgmt_info.controller_vtep_mac = controller_vtep_mac
        # Set the controller VTEP IP
        if controller_vtep_ip is not None:
            reply.mgmt_info.controller_vtep_ip = controller_vtep_ip
        # Set the device VTEP IP
        if device_vtep_ip is not None:
            reply.mgmt_info.device_vtep_ip = device_vtep_ip
        # Set the VTEP mask
        if vtep_mask is not None:
            reply.mgmt_info.vtep_mask = vtep_mask
        # Send the reply
        logging.info('Sending the reply ( UpdateMgmtInfo ) : %s' % reply)
        return reply

    def UnregisterDevice(self, request, context):
        logging.info('Unregister device request: %s' % request)
        # Extract the parameters from the registration request
        #
        # Device ID
        deviceid = request.device.id
        # Tenant ID
        tenantid = request.tenantid
        # Unregister the device
        logging.debug('Trying to unregister the device %s', deviceid)
        response = self.controller.unregister_device(deviceid, tenantid)
        if response is not STATUS_SUCCESS:
            return pymerang_pb2.RegisterDeviceReply(status=response)
        # Send the reply
        reply = pymerang_pb2.RegisterDeviceReply(status=STATUS_SUCCESS)
        logging.info('Sending the reply ( UnregisterDevice ) : %s', reply)
        return reply

    def KeepAlive(self, request, context):
        logging.debug('Received keep alive message on the gRPC channel')
        # Device ID
        deviceid = request.device.id
        # Tenant ID
        tenantid = request.tenantid
        # Get the device
        device = srv6_sdn_controller_state.get_device(
            deviceid=deviceid, tenantid=tenantid
        )
        if device is None:
            logging.error('Error getting device')
            return status_codes_pb2.STATUS_INTERNAL_ERROR
        # Report the status to the device
        reply = pymerang_pb2.RegisterDeviceReply(
            status=STATUS_SUCCESS,
            device_state=device.get('state', DeviceState.UNKNOWN.value)
        )
        logging.debug('Sending the reply (KeepAlive) : %s', reply)
        return reply

    def ExecReconciliation(self, request, context):
        logging.debug(
            'Received ExecReconciliation message on the gRPC channel'
        )
        # Device ID
        deviceid = request.device.id
        # Tenant ID
        tenantid = request.tenantid
        # Execute reconciliation
        res = self.controller.exec_reconciliation(
            deviceid=deviceid, tenantid=tenantid
        )
        if res != STATUS_SUCCESS:
            logging.error('Error in exec_reconciliation')
        # Get the device
        device = srv6_sdn_controller_state.get_device(deviceid, tenantid)
        if device is None:
            logging.error('Device %s not found' % deviceid)
            return STATUS_INTERNAL_ERROR
        # Report the status to the device
        reply = pymerang_pb2.RegisterDeviceReply(
            status=res,
            device_state=device.get(
                'state',
                device.get('state', DeviceState.UNKNOWN.value)
            )
        )
        logging.debug('Sending the reply (ExecReconciliation): %s', reply)
        return reply


class PymerangController:

    def __init__(
        self,
        server_ip='::1',
        server_port=50051,
        keep_alive_interval=DEFAULT_KEEP_ALIVE_INTERVAL,
        max_keep_alive_lost=DEFAULT_MAX_KEEP_ALIVE_LOST,
        secure=DEFAULT_SECURE,
        key=DEFAULT_KEY,
        certificate=DEFAULT_CERTIFICATE,
        nb_interface_ref=None
    ):
        # IP address on which the gRPC listens for connections
        self.server_ip = server_ip
        # Port used by the gRPC server
        self.server_port = server_port
        # Tunnel state
        self.tunnel_state = None
        # Interval between two consecutive keep alive messages
        self.keep_alive_interval = keep_alive_interval
        # Max keep alive lost
        self.max_keep_alive_lost = max_keep_alive_lost
        # Secure mode
        self.secure = secure
        # Server key
        self.key = key
        # Certificate
        self.certificate = certificate
        # Reference to the Northbound interface
        self.nb_interface_ref = nb_interface_ref
        # Devices connected
        self.connected_devices = dict()

    # Restore management interfaces, if any
    def restore_mgmt_interfaces(self):
        logging.info('*** Restoring management interfaces')
        # Get all the devices
        devices = srv6_sdn_controller_state.get_devices()
        if devices is None:
            logging.error('Cannot retrieve devices list')
            return

        # Iterate on the devices list
        for device in devices:
            # Get the ID of the device
            deviceid = device['deviceid']
            # Get the ID of the tenant
            tenantid = device['tenantid']
            # Get the tunnel mode used for this device
            tunnel_mode = device.get('tunnel_mode')
            # If tunnel mode is valid, restore the tunnel endpoint
            if tunnel_mode is not None:
                logging.info(
                    'Restoring management interface for device %s',
                    device['deviceid']
                )
                if device.get('external_ip') is not None:
                    device_external_ip = device['external_ip']
                if device.get('external_port') is not None:
                    device_external_port = device['external_port']
                if device.get('mgmt_mac') is not None:
                    device_vtep_mac = device['mgmt_mac']
                if device.get('vxlan_port') is not None:
                    vxlan_port = device['vxlan_port']
                # Create tunnel controller endpoint
                tunnel_mode = self.tunnel_state.tunnel_modes[tunnel_mode]
                res, controller_vtep_mac, controller_vtep_ip, device_vtep_ip, \
                    vtep_mask = tunnel_mode.create_tunnel_controller_endpoint(
                        deviceid=deviceid,
                        tenantid=tenantid,
                        device_external_ip=device_external_ip,
                        device_external_port=device_external_port,
                        vxlan_port=vxlan_port,
                        device_vtep_mac=device_vtep_mac
                    )
                if res != STATUS_SUCCESS:
                    logging.warning(
                        'Cannot restore the tunnel on device %s', deviceid
                    )
                    return res
        # Success
        return STATUS_SUCCESS

    # Authenticate a device
    def authenticate_device(self, deviceid, auth_data):
        logging.info('Authenticating the device %s' % deviceid)
        # Get token
        token = auth_data.token
        # Authenticate the device
        authenticated, tenantid = (
            srv6_sdn_controller_state.authenticate_device(token)
        )
        if not authenticated:
            return False, None
        # Return the tenant ID
        return True, tenantid

    # Register a device
    def register_device(
        self,
        deviceid,
        features,
        interfaces,
        mgmtip,
        auth_data,
        sid_prefix=None,
        public_prefix_length=None,
        enable_proxy_ndp=True,
        force_ip6tnl=False,
        force_srh=False,
        incoming_sr_transparency=None,
        outgoing_sr_transparency=None
    ):
        logging.info('Registering the device %s', deviceid)
        # Device authentication
        authenticated, tenantid = self.authenticate_device(
            deviceid, auth_data
        )
        if not authenticated:
            logging.info('Authentication failed for the device %s' % deviceid)
            return STATUS_UNAUTHORIZED, None, None
        # If the device is already registered, send it the configuration
        # and create tunnels
        if srv6_sdn_controller_state.device_exists(deviceid, tenantid):
            logging.warning('The device %s is already registered' % deviceid)
            srv6_sdn_controller_state.set_device_reconciliation_flag(
                deviceid, tenantid, flag=True
            )
        else:
            # Update controller state
            srv6_sdn_controller_state.register_device(
                deviceid,
                features,
                interfaces,
                mgmtip,
                tenantid,
                sid_prefix,
                public_prefix_length,
                enable_proxy_ndp,
                force_ip6tnl,
                force_srh,
                incoming_sr_transparency,
                outgoing_sr_transparency
            )
        # Get the tenant configuration
        config = srv6_sdn_controller_state.get_tenant_config(tenantid)
        if config is None:
            logging.error(
                'Tenant not found or error while connecting to the db'
            )
            return STATUS_INTERNAL_ERROR, None, None
        # Set the port
        vxlan_port = config.get('vxlan_port', DEFAULT_VXLAN_PORT)
        # Success
        logging.debug('New device registered:\n%s' % deviceid)
        return STATUS_SUCCESS, vxlan_port, tenantid

    def reconciliation_failed(self, deviceid, tenantid):
        logging.error('Reconciliation has failed for device %s.', deviceid)
        # Reconciliation has failed, we need to reboot the device to bring it
        # in a consistent state
        # Change device state to reboot required
        success = srv6_sdn_controller_state.change_device_state(
            deviceid=deviceid,
            tenantid=tenantid,
            new_state=DeviceState.REBOOT_REQUIRED
        )
        if success is False or success is None:
            logging.error('Error changing the device state')
            return status_codes_pb2.STATUS_INTERNAL_ERROR
        # Increase reconciliation failures counter
        failures = (
            srv6_sdn_controller_state.inc_and_get_reconciliation_failures(
                deviceid=deviceid, tenantid=tenantid
            )
        )
        logging.error('%s failures.', failures)
        if failures >= MAX_ALLOWED_RECONCILIATION_FAILURES:
            # TODO force device disactivation
            logging.error(
                'Too many failures for device %s (%d failures)',
                deviceid,
                failures
            )
            # Change device state to failure state
            success = srv6_sdn_controller_state.change_device_state(
                deviceid=deviceid,
                tenantid=tenantid,
                new_state=DeviceState.FAILURE
            )
            if success is False or success is None:
                logging.error('Error changing the device state')
                return status_codes_pb2.STATUS_INTERNAL_ERROR
        else:
            logging.error('Trying to reboot device %s', deviceid)
            if srv6_sdn_controller_state.can_reboot_device(
                deviceid=deviceid, tenantid=tenantid
            ):
                # device = srv6_sdn_controller_state.get_device(
                #     deviceid=deviceid, tenantid=tenantid)
                # if device is None:
                #     logging.error('Error getting device')
                #     return status_codes_pb2.STATUS_INTERNAL_ERROR
                # self.srv6_manager.reboot_device(
                #     server_ip=device['mgmtip'],
                #     server_port=self.grpc_client_port)
                # # Change device state to reboot required
                # success = srv6_sdn_controller_state.change_device_state(
                #     deviceid=deviceid, tenantid=tenantid,
                #     new_state=srv6_sdn_controller_state.DeviceState.REBOOTING)
                # if success is False or success is None:
                #     logging.error('Error changing the device state')
                #     return status_codes_pb2.STATUS_INTERNAL_ERROR
                #
                # TODO currently, the state is communicated to the device in
                # the reply to the update tunnel message, it is responsibility
                # of the device to reboot itself if the state is reboot
                # required maybe we can implement a reboot RPC in the future...
                logging.error('Reboot is required for device %s', deviceid)
            else:
                # Reboot is disabled for the device, manual reboot is required
                # to bring the device in a consistent state
                logging.error('Reboot is disabled for device %s', deviceid)
                logging.error(
                    'Manual reboot is required to start the '
                    'reconciliation procedure and bring the device '
                    'in a consistent state. Please reboot the device '
                    'manually.'
                )

    # Update tunnel mode
    def update_mgmt_info(
        self,
        deviceid,
        tenantid,
        interfaces,
        mgmtip,
        tunnel_name,
        nat_type,
        device_external_ip,
        device_external_port,
        device_vtep_mac,
        vxlan_port
    ):
        logging.info(
            'Updating the management information for the device %s', deviceid
        )
        # If a tunnel already exists, we need to destroy it
        # before creating the new tunnel
        old_tunnel_mode = srv6_sdn_controller_state.get_tunnel_mode(
            deviceid, tenantid
        )
        if old_tunnel_mode is not None:
            old_tunnel_mode = self.tunnel_state.tunnel_modes[old_tunnel_mode]
            res = old_tunnel_mode.destroy_tunnel_controller_endpoint(
                deviceid, tenantid
            )
            if res != status_codes_pb2.STATUS_SUCCESS:
                logging.error(
                    'Error during destroy_tunnel_controller_endpoint'
                )
                return res, None, None, None, None
            srv6_sdn_controller_state.set_tunnel_mode(deviceid, tenantid, None)
        # Get the tunnel mode requested by the device
        tunnel_mode = self.tunnel_state.tunnel_modes[tunnel_name]
        # Create the tunnel
        logging.info(
            'Trying to create the tunnel for the device %s', deviceid
        )
        res, controller_vtep_mac, controller_vtep_ip, device_vtep_ip, \
            vtep_mask = tunnel_mode.create_tunnel_controller_endpoint(
                deviceid=deviceid,
                tenantid=tenantid,
                device_external_ip=device_external_ip,
                device_external_port=device_external_port,
                vxlan_port=vxlan_port,
                device_vtep_mac=device_vtep_mac
            )
        if res != STATUS_SUCCESS:
            logging.warning('Cannot create the tunnel')
            return res, None, None, None, None
        # If a private IP address is present, use it as mgmt address
        res = srv6_sdn_controller_state.get_device_mgmtip(tenantid, deviceid)
        if res is not None:
            mgmtip = srv6_sdn_controller_state.get_device_mgmtip(
                tenantid, deviceid
            ).split('/')[0]
        # If a keep alive thread for the device is already running, we
        # need to stop it before starting new keep keep alive thread
        # to prevent race conditions
        thread_id = f'{tenantid}/{deviceid}'
        if self.connected_devices.get(thread_id) is not None:
            self.connected_devices[thread_id].set()
        # Create a new event, used to stop the Keep Alive procedure
        stop_event = Event()
        self.connected_devices[f'{tenantid}/{deviceid}'] = stop_event
        # Send a keep-alive messages to keep the tunnel opened,
        # if required for the tunnel mode
        # After N keep alive messages lost, we assume that the device
        # is not reachable, and we mark it as "not connected"
        if tunnel_mode.require_keep_alive_messages:
            Thread(
                target=utils.start_keep_alive_icmp,
                args=(
                    mgmtip,
                    self.keep_alive_interval,
                    self.max_keep_alive_lost,
                    stop_event,
                    lambda: self.device_disconnected(deviceid, tenantid)
                ),
                daemon=False
            ).start()
        # Update controller state
        success = srv6_sdn_controller_state.update_mgmt_info(
            deviceid,
            tenantid,
            mgmtip,
            interfaces,
            tunnel_name,
            nat_type,
            device_external_ip,
            device_external_port,
            device_vtep_mac,
            vxlan_port
        )
        if success is None or success is False:
            err = (
                'Cannot update tunnel management info. '
                'Error while updating the controller state'
            )
            logging.error(err)
            return STATUS_INTERNAL_ERROR, None, None, None, None
        # Mark the device as "connected"
        success = srv6_sdn_controller_state.set_device_connected_flag(
            deviceid=deviceid, tenantid=tenantid, connected=True
        )
        if success is None or success is False:
            err = (
                'Cannot set the device as connected. '
                'Error while updating the controller state'
            )
            logging.error(err)
            return STATUS_INTERNAL_ERROR, None, None, None, None
        # Update gRPC IP used by STAMP
        stamp_node = (
            self.nb_interface_ref.stamp_controller.storage.get_stamp_node(
                node_id=deviceid, tenantid=tenantid
            )
        )
        if stamp_node is not None:
            self.nb_interface_ref.stamp_controller.storage.update_stamp_node(
                node_id=deviceid, tenantid=tenantid, grpc_ip=mgmtip
            )
        # Device registration and authentication completed successfully,
        # now it is working
        success = srv6_sdn_controller_state.change_device_state(
            deviceid=deviceid,
            tenantid=tenantid,
            new_state=DeviceState.WORKING
        )
        if success is False or success is None:
            logging.error('Error changing the device state')
            return STATUS_INTERNAL_ERROR, None, None, None, None
        # Success
        logging.debug('Updated management information: %s', deviceid)
        return (
            STATUS_SUCCESS,
            controller_vtep_mac,
            controller_vtep_ip,
            device_vtep_ip,
            vtep_mask
        )

    def unregister_device(self, deviceid, tenantid):
        logging.debug('Unregistering the device %s', deviceid)
        # Get the device
        device = srv6_sdn_controller_state.get_device(deviceid, tenantid)
        if device is None:
            logging.error('Device %s not found', deviceid)
            return STATUS_INTERNAL_ERROR
        # Get tunnel mode
        tunnel_mode = device['tunnel_mode']
        if tunnel_mode is not None:
            # Get the tunnel mode class from its name
            tunnel_mode = self.tunnel_state.tunnel_modes[tunnel_mode]
            # Destroy the tunnel
            logging.debug(
                'Trying to destroy the tunnel for the device %s', deviceid
            )
            res = tunnel_mode.destroy_tunnel_controller_endpoint(
                deviceid, tenantid
            )
            if res != status_codes_pb2.STATUS_SUCCESS:
                logging.error(
                    'Error during destroy_tunnel_controller_endpoint'
                )
                return res
        # Success
        logging.debug('Device unregistered: %s', deviceid)
        return STATUS_SUCCESS

    def device_disconnected(self, deviceid, tenantid):
        logging.debug('The device %s has been disconnected', deviceid)
        # Get the device
        device = srv6_sdn_controller_state.get_device(deviceid, tenantid)
        if device is None:
            logging.error('Device %s not found', deviceid)
            return STATUS_INTERNAL_ERROR
        # Mark the device as "not connected"
        success = srv6_sdn_controller_state.set_device_connected_flag(
            deviceid=deviceid, tenantid=tenantid, connected=False
        )
        if success is None or success is False:
            err = (
                'Cannot set the device as disconnected. '
                'Error while updating the controller state'
            )
            logging.error(err)
            return STATUS_INTERNAL_ERROR
        # Get tunnel mode
        tunnel_mode = device['tunnel_mode']
        if tunnel_mode is not None:
            # Get the tunnel mode class from its name
            tunnel_mode = self.tunnel_state.tunnel_modes[tunnel_mode]
            # Destroy the tunnel
            logging.debug(
                'Trying to destroy the tunnel for the device %s', deviceid
            )
            res = tunnel_mode.destroy_tunnel_controller_endpoint(
                deviceid, tenantid
            )
            if res != status_codes_pb2.STATUS_SUCCESS:
                logging.error(
                    'Error during destroy_tunnel_controller_endpoint'
                )
                return res
            srv6_sdn_controller_state.set_tunnel_mode(deviceid, tenantid, None)
        # Clear management information on the database
        srv6_sdn_controller_state.clear_mgmt_info(deviceid, tenantid)
        # Remove keep alive stop event
        thread_id = f'{tenantid}/{deviceid}'
        del self.connected_devices[thread_id]
        # Success
        logging.debug('Device disconnected: %s' % deviceid)
        return STATUS_SUCCESS

    def exec_reconciliation(self, deviceid, tenantid):
       
        if srv6_sdn_controller_state.get_device_reconciliation_flag(
                deviceid=deviceid, tenantid=tenantid
        ):
            
            with RollbackContext() as rollback:
                rollback.push(
                    func=self.reconciliation_failed,
                    deviceid=deviceid,
                    tenantid=tenantid
                )
                res = (
                    self.nb_interface_ref.prepare_db_for_device_reconciliation(
                        deviceid=deviceid, tenantid=tenantid
                    )
                )
                if res != 200:
                    return res
                res = self.nb_interface_ref.device_reconciliation(
                    deviceid=deviceid, tenantid=tenantid
                )
                if res != 200:
                    return res
                res = self.nb_interface_ref.overlay_reconciliation(
                    deviceid=deviceid, tenantid=tenantid
                )
                if res != 200:
                    return res
                srv6_sdn_controller_state.set_device_reconciliation_flag(
                    deviceid, tenantid, flag=False
                )
                # Success, commit all performed operations
                rollback.commitAll()
        # Reconciliation successful, reset the failures counter
        success = srv6_sdn_controller_state.reset_reconciliation_failures(
            deviceid=deviceid, tenantid=tenantid
        )
        if success is None or success is False:
            err = (
                'Cannot reset the reconciliation failures counter for '
                'device %s. Error while updating the controller state',
                deviceid
            )
            logging.error(err)
            return STATUS_INTERNAL_ERROR
        return STATUS_SUCCESS

    def serve(self):
        # Initialize tunnel state
        self.tunnel_state = utils.TunnelState(self.server_ip)
        # Restore management interfaces, if any
        self.restore_mgmt_interfaces()
        # Start gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pymerang_pb2_grpc.add_PymerangServicer_to_server(
            PymerangServicer(self), server
        )
        if tunnel_utils.getAddressFamily(self.server_ip) == AF_INET6:
            server_address = '[%s]:%s' % (self.server_ip, self.server_port)
        elif tunnel_utils.getAddressFamily(self.server_ip) == AF_INET:
            server_address = '%s:%s' % (self.server_ip, self.server_port)
        else:
            logging.error('Invalid server address %s' % self.server_ip)
            return
        # If secure mode is enabled, we need to create a secure endpoint
        if self.secure:
            # Read key and certificate
            with open(self.key, 'rb') as f:
                key = f.read()
            with open(self.certificate, 'rb') as f:
                certificate = f.read()
            # Create server SSL credentials
            grpc_server_credentials = grpc.ssl_server_credentials(
                ((key, certificate,),)
            )
            # Create a secure endpoint
            server.add_secure_port(
                server_address,
                grpc_server_credentials
            )
        else:
            # Create an insecure endpoint
            server.add_insecure_port(server_address)
        # Start the loop for gRPC
        logging.info('Server started: listening on %s', server_address)
        server.start()
        # Wait for server termination
        while True:
            time.sleep(10)


# Parse options
def parse_arguments():
    # Get parser
    parser = ArgumentParser(
        description='pymerang server'
    )
    # Debug mode
    parser.add_argument(
        '-d', '--debug', action='store_true', help='Activate debug logs'
    )
    # Secure mode
    parser.add_argument(
        '-s', '--secure', action='store_true', help='Activate secure mode'
    )
    # gRPC server IP
    parser.add_argument(
        '-i',
        '--server-ip',
        dest='server_ip',
        default=DEFAULT_PYMERANG_SERVER_IP,
        help='Server IP address'
    )
    # gRPC server port
    parser.add_argument(
        '-p',
        '--server-port',
        dest='server_port',
        default=DEFAULT_PYMERANG_SERVER_PORT,
        help='Server port'
    )
    # Interval between two consecutive keep alive messages
    parser.add_argument(
        '-a',
        '--keep-alive-interval',
        dest='keep_alive_interval',
        default=DEFAULT_KEEP_ALIVE_INTERVAL,
        help='Interval between two consecutive keep alive'
    )
    # Interval between two consecutive keep alive messages
    parser.add_argument(
        '-m',
        '--max-keep-alive-lost',
        dest='max_keep_alive_lost',
        default=DEFAULT_MAX_KEEP_ALIVE_LOST,
        help='Interval between two consecutive keep alive'
    )
    # Server certificate file
    parser.add_argument(
        '-c',
        '--certificate',
        dest='certificate',
        action='store',
        default=DEFAULT_CERTIFICATE,
        help='Server certificate file'
    )
    # Server key
    parser.add_argument(
        '-k',
        '--key',
        dest='key',
        action='store',
        default=DEFAULT_KEY,
        help='Server key file'
    )
    # Parse input parameters
    args = parser.parse_args()
    # Return the arguments
    return args


if __name__ == '__main__':
    args = parse_arguments()
    # Setup properly the logger
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().setLevel(level=logging.INFO)
    # Setup properly the secure mode
    if args.secure:
        secure = True
    else:
        secure = False
    # Server certificate file
    certificate = args.certificate
    # Server key
    key = args.key
    # gRPC server IP
    server_ip = args.server_ip
    # gRPC server port
    server_port = args.server_port
    # Keep alive interval
    keep_alive_interval = args.keep_alive_interval
    # Max keep alive lost
    max_keep_alive_lost = args.max_keep_alive_lost
    # Start server
    controller = PymerangController(
        server_ip,
        server_port,
        keep_alive_interval,
        max_keep_alive_lost,
        secure,
        key,
        certificate
    )
    controller.serve()
