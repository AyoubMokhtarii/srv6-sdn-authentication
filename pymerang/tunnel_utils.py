#!/usr/bin/env python

# General imports
import logging
import socket
import time
from ipaddress import IPv6Interface, IPv4Interface, AddressValueError
# pyroute2 dependencies
from pyroute2 import IPRoute
from pyroute2.netlink.rtnl import ndmsg
from pyroute2.netlink.exceptions import NetlinkError


def enable_interface(device):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get interface index
        ifindex = ip_route.link_lookup(ifname=device)[0]
        # Bring link up
        ip_route.link("set", index=ifindex, state="up")


def disable_interface(device):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get interface index
        ifindex = ip_route.link_lookup(ifname=device)[0]
        # Put link down
        ip_route.link("set", index=ifindex, state="down")


def delete_interface(device):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get interface index
        ifindex = ip_route.link_lookup(ifname=device)[0]

        #FIXME remove this logging
        print("******************************************** ifname=device :  ",device)
        print("******************************************** ifindex :  ",ifindex)
        time.sleep(10)

        # Delete link
        ip_route.link("del", index=ifindex)


def add_address(device, address, mask):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get interface index
        ifindex = ip_route.link_lookup(ifname=device)[0]
        # Add the address
        ip_route.addr('add', index=ifindex, address=address, mask=mask)


def del_address(device, address, mask):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get interface index
        ifindex = ip_route.link_lookup(ifname=device)[0]
        # Add the address
        ip_route.addr('del', index=ifindex, address=address, mask=mask)


def add_route(dst, gateway, dev, family):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Create the route
        ip_route.route(
            'add',
            dst=dst,
            oif=ip_route.link_lookup(ifname=dev)[0],
            gateway=gateway,
            family=family
        )


def get_mac_address(ifname):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get MAC address
        return ip_route.get_links(ifname=ifname)[0].get_attr('IFLA_ADDRESS')


def create_ip_neigh(dst, lladdr, dev):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Add a permanent record on veth0
        idx = ip_route.link_lookup(ifname=dev)[0]
        # Create the neigh
        ip_route.neigh(
            'add',
            dst=dst,
            lladdr=lladdr,
            ifindex=idx,
            state=ndmsg.states['permanent']
        )


def update_ip_neigh(dst, lladdr, dev):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Add a permanent record on veth0
        idx = ip_route.link_lookup(ifname=dev)[0]
        # Create the neigh
        ip_route.neigh(
            'replace',
            dst=dst,
            lladdr=lladdr,
            ifindex=idx,
            state=ndmsg.states['permanent']
        )


def remove_ip_neigh(dst, dev):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get the index of the interface
        idx = ip_route.link_lookup(ifname=dev)[0]
        # Remove the neigh
        ip_route.neigh(
            'del',
            dst=dst,
            ifindex=idx
        )


def create_or_update_ip_neigh(dst, lladdr, dev):
    try:
        logging.debug(
            'Trying to create the IP neigh, dst %s, lladdr %s, dev %s',
            dst,
            lladdr,
            dev
        )
        return create_ip_neigh(dst, lladdr, dev)
    except NetlinkError as e:
        if e.code == 17:
            logging.debug('The IP neigh entity already exists')
            logging.debug('Updating existing IP neigh')
            return update_ip_neigh(dst, lladdr, dev)
        raise(e)


# Utiliy function to check if the IP
# is a valid IPv6 address
def validate_ipv6_address(ip):
    if ip is None:
        return False
    try:
        IPv6Interface(ip)
        return True
    except AddressValueError:
        return False


# Utiliy function to check if the IP
# is a valid IPv4 address
def validate_ipv4_address(ip):
    if ip is None:
        return False
    try:
        IPv4Interface(ip)
        return True
    except AddressValueError:
        return False


# Utiliy function to get the IP address family
def getAddressFamily(ip):
    if validate_ipv6_address(ip):
        # IPv6 address
        return socket.AF_INET6
    elif validate_ipv4_address(ip):
        # IPv4 address
        return socket.AF_INET
    else:
        # Invalid address
        return None


class TunnelMode:

    '''
    Class representing a tunnel mode use for controller-device communication
    '''

    def __init__(
        self,
        name,
        require_keep_alive_messages,
        supported_nat_types,
        priority,
        controller_ip,
        debug=False
    ):
        logging.info(
            'Initiating the tunnel mode:\n'
            'name=%s\npriority=%s\ncontroller_ip=%s\n'
            'require_keep_alive_messages=%s\n'
            'supported_nat_types=%s\n',
            name,
            priority,
            controller_ip,
            require_keep_alive_messages,
            supported_nat_types
        )
        # Debug mode
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
        # Name used to identify tunnel mode
        self.name = name
        # True if the tunnel mode require keep alive messages
        # sent over the tunnel
        self.require_keep_alive_messages = require_keep_alive_messages
        # NAT types supported by the tunnel mode
        self.supported_nat_types = supported_nat_types
        # Priority index, lower is better
        self.priority = priority
        # # Public IP address of the controller
        self.controller_ip = controller_ip
        # # # Private IPv6 net allocator
        # # self.ipv6_net_allocator = ipv6_net_allocator
        # # Private IPv4 net allocator
        # self.ipv4_net_allocator = ipv4_net_allocator
        # # Private IPv6 address allocator
        # self.ipv6_address_allocator = ipv6_address_allocator
        # # Private IPv4 address allocator
        # self.ipv4_address_allocator = ipv4_address_allocator
        # # Mapping device ID to private IPv4 nets (stored in the controller)
        # self.device_to_ipv4_net = dict()
        # # Mapping device ID to private IPv6 nets (stored in the controller)
        # self.device_to_ipv6_net = dict()
        # # Set of reusable IPv4 nets
        # self.reusable_ipv4_nets = set()
        # # Set of reusable IPv6 nets
        # self.reusable_ipv6_nets = set()
        # # Mapping device ID to private IPv4 address (stored in the
        # # controller)
        # self.device_to_mgmt_ipv4 = dict()
        # # Mapping device ID to private IPv6 address (stored in the
        # # controller)
        # self.device_to_mgmt_ipv6 = dict()
        # # Set of reusable IPv4 addresses
        # self.reusable_ipv4_addresses = set()
        # # Set of reusable IPv6 addresses
        # self.reusable_ipv6_addresses = set()
        # # Private IPv4 address of the controller (stored in the controller)
        self.controller_mgmtipv4 = None
        # Private IPv6 address of the controller (store in the controller)
        self.controller_mgmtipv6 = None
        # Private IP address of the controller (stored in the device)
        self.controller_mgmtip = None
        # # Device to MAC address
        # # self.device_to_mac_addr = dict()

    # Invoked on the device, before the registration request
    def create_tunnel_device_endpoint(self, deviceid, tenantid, vxlan_port):
        raise NotImplementedError

    # Invoked on the device, after the controller reply
    def create_tunnel_device_endpoint_end(
        self,
        deviceid,
        tenantid,
        controller_vtep_ip,
        device_vtep_ip,
        vtep_mask,
        controller_vtep_mac
    ):
        raise NotImplementedError

    # Invoked on the controller, when the registration request is received
    def create_tunnel_controller_endpoint(
        self,
        deviceid,
        tenantid,
        device_external_ip,
        device_external_port,
        vxlan_port,
        device_vtep_mac
    ):
        raise NotImplementedError

    # Invoked on the device, when the tunnel has to be destroyed
    def destroy_tunnel_device_endpoint(self, deviceid, tenantid):
        raise NotImplementedError

    # Invoked on the device, when the tunnel has to be destroyed
    def destroy_tunnel_device_endpoint_end(self, deviceid, tenantid):
        raise NotImplementedError

    # Invoked on the controller, when the tunnel has to be destroyed
    def destroy_tunnel_controller_endpoint(self, deviceid, tenantid):
        raise NotImplementedError

    # Invoked on the device, before the update request
    def update_tunnel_device_endpoint(
        self,
        deviceid,
        tenantid,
        controller_vtep_ip,
        device_vtep_ip,
        vtep_mask,
        controller_vtep_mac
    ):
        raise NotImplementedError

    # Invoked on the device, after the controller reply
    def update_tunnel_device_endpoint_end(
        self,
        deviceid,
        tenantid,
        controller_vtep_ip,
        device_vtep_ip,
        vtep_mask,
        controller_vtep_mac
    ):
        raise NotImplementedError

    # Invoked on the controller, when the update request is received
    def update_tunnel_controller_endpoint(
        self,
        deviceid,
        tenantid,
        device_external_ip,
        device_external_port,
        device_vtep_mask,
        vxlan_port,
        device_vtep_mac
    ):
        raise NotImplementedError

    # # Return the private IPv6 of the device
    # def get_device_mgmtipv6(self, tenantid, deviceid):
    #     if tenantid not in self.device_to_mgmt_ipv6:
    #         return None
    #     return self.device_to_mgmt_ipv6[tenantid].get(deviceid)

    # # Return the private IPv4 of the device
    # def get_device_mgmtipv4(self, tenantid, deviceid):
    #     if tenantid not in self.device_to_mgmt_ipv4:
    #         return None
    #     return self.device_to_mgmt_ipv4[tenantid].get(deviceid)

    # # Return the private IP of the device
    # def get_device_mgmtip(self, tenantid, deviceid):
    #     addr = self.get_device_mgmtipv4(tenantid, deviceid)
    #     if addr is None:
    #         addr = self.get_device_mgmtipv6(tenantid, deviceid)
    #     return addr

    # # Return the public IP of the controller
    def get_controller_ip(self):
        return self.controller_ip

    # Return the private IP of the controller
    def get_controller_mgmtip(self):
        return self.controller_mgmtip

    # # Initiate tenant ID
    # # def init_tenantid(self, tenantid):
    # #     if self.device_to_mgmt_ipv4 is not None:
    #         self.device_to_mgmt_ipv4[tenantid] = dict()
    #     if self.device_to_mgmt_ipv6 is not None:
    #         self.device_to_mgmt_ipv6[tenantid] = dict()
    #     if self.device_to_ipv4_net is not None:
    #         self.device_to_ipv4_net[tenantid] = dict()
    #     if self.device_to_ipv6_net is not None:
    #         self.device_to_ipv6_net[tenantid] = dict()

    # Release tenant ID
    # def release_tenantid(self, tenantid):
    #     if self.device_to_mgmt_ipv4:
    #         del self.device_to_mgmt_ipv4[tenantid]
    #     if self.device_to_mgmt_ipv6:
    #         del self.device_to_mgmt_ipv6[tenantid]
    #     if self.device_to_ipv4_net:
    #         del self.device_to_ipv4_net[tenantid]
    #     if self.device_to_ipv6_net:
    #         del self.device_to_ipv6_net[tenantid]

    # # Allocate a new private IPv4 address for the device
    # # If the device already has a IPv4 address, return it
    # def get_new_mgmt_ipv4(self, deviceid, tenantid):
    #     if deviceid in self.device_to_mgmt_ipv4[tenantid]:
    #         return self.device_to_mgmt_ipv4[tenantid][deviceid]
    #     elif len(self.reusable_ipv4_addresses) > 0:
    #         addr = self.reusable_ipv4_addresses.pop()
    #     else:
    #         addr = '%s/%s' % (self.ipv4_address_allocator.nextAddress(),
    #                           self.ipv4_address_allocator.prefix)
    #     self.device_to_mgmt_ipv4[tenantid][deviceid] = addr
    #     return addr

    # # Allocate a new private IPv6 address for the device
    # # If the device already has a IPv4 address, return it
    # def get_new_mgmt_ipv6(self, deviceid, tenantid):
    #     if deviceid in self.device_to_mgmt_ipv6[tenantid]:
    #         return self.device_to_mgmt_ipv6[tenantid][deviceid]
    #     elif len(self.reusable_ipv6_addresses) > 0:
    #         addr = self.reusable_ipv6_addresses.pop()
    #     else:
    #         addr = '%s/%s' % (self.ipv6_address_allocator.nextAddress(),
    #                           self.ipv6_address_allocator.prefix)
    #     self.device_to_mgmt_ipv6[tenantid][deviceid] = addr
    #     return addr

    # # Release the IPv4 address associated to the device
    # def release_ipv4_address(self, deviceid, tenantid):
    #     addr = self.device_to_mgmt_ipv4[tenantid].pop(deviceid, None)
    #     if addr is not None:
    #         self.reusable_ipv4_addresses.add(addr)

    # # Release the IPv6 address associated to the device
    # def release_ipv6_address(self, deviceid, tenantid):
    #     addr = self.device_to_mgmt_ipv6[tenantid].pop(deviceid, None)
    #     if addr is not None:
    #         self.reusable_ipv6_addresses.add(addr)

    # # Get a new private IPv4 net for the device-controller communication
    # # If the device already has a IPv4 net, return it
    # def get_new_mgmt_ipv4_net(self, deviceid):
    #     if deviceid in self.device_to_ipv4_net:
    #         return self.device_to_ipv4_net[deviceid]
    #     elif len(self.reusable_ipv4_nets) > 0:
    #         net = self.reusable_ipv4_nets.pop()
    #     else:
    #         net = self.ipv4_net_allocator.nextNet()
    #     self.device_to_ipv4_net[deviceid] = net
    #     return net

    # # Get a new private IPv6 net for the device-controller communication
    # # If the device already has a IPv6 net, return it
    # def get_new_mgmt_ipv6_net(self, deviceid):
    #     if deviceid in self.device_to_ipv6_net:
    #         return self.device_to_ipv6_net[deviceid]
    #     elif len(self.reusable_ipv6_nets) > 0:
    #         net = self.reusable_ipv6_nets.pop()
    #     else:
    #         net = self.ipv6_net_allocator.nextNet()
    #     self.device_to_ipv6_net[deviceid] = net
    #     return net

    # # Release the IPv4 net associated to the device
    # def release_ipv4_net(self, deviceid):
    #     net = self.device_to_ipv4_net.pop(deviceid, None)
    #     if net is not None:
    #         self.reusable_ipv4_nets.add(net)

    # # Release the IPv6 net associated to the device
    # def release_ipv6_net(self, deviceid):
    #     net = self.device_to_ipv6_net.pop(deviceid, None)
    #     if net is not None:
    #         self.reusable_ipv6_nets.add(net)

    # # Get the number of addresses allocated for a tenant
    # def num_addresses(self, tenantid):
    #     return len(self.device_to_mgmt_ipv6.get(tenantid, set())) + \
    #         len(self.device_to_mgmt_ipv4.get(tenantid, set()))

    # # Get the MAC address of the VTEP
    # def get_device_vtep_mac(self, deviceid):
    #     return self.device_to_mac_addr.get(deviceid)
