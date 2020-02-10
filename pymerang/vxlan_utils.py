#!/usr/bin/env python

# General imports
import logging
import socket
import pynat
# pyroute2 dependencies
from pyroute2 import IPRoute
# pymerang dependencies
from pymerang import tunnel_utils
from pymerang import status_codes_pb2

# Destination port used by VXLAN interfaces
VXLAN_DSTPORT = 4789
# Enable UDP checksum on VXLAN packets
ENABLE_UDP_CSUM = True
# VNI used for the management VTEPs
MGMT_VNI = 0


def create_vxlan(device, vni, phys_dev=None, remote=None,
                 local=None, remote6=None, local6=None,
                 ttl=None, tos=None, flowlabel=None,
                 dstport=None, srcport_min=None,
                 srcport_max=None, learning=None, proxy=None,
                 rsc=None, l2miss=None, l3miss=None,
                 udpcsum=None, udp6zerocsumtx=None,
                 udp6zerocsumrx=None, ageing=None,
                 gbp=None, gpe=None):
    '''
    From the Manpage:

    ip link add DEVICE type vxlan id VNI [ dev PHYS_DEV ] [ { group |
    remote } IPADDR ] [ local { IPADDR | any } ] [ ttl TTL ] [ tos TOS ]
    [ flowlabel FLOWLABEL ] [ dstport PORT ] [ srcport MIN MAX ] [
    [no]learning ] [ [no]proxy ] [ [no]rsc ] [ [no]l2miss ] [ [no]l3miss
    ] [ [no]udpcsum ] [ [no]udp6zerocsumtx ] [ [no]udp6zerocsumrx ] [
    ageing SECONDS ] [ maxaddress NUMBER ] [ [no]external ] [ gbp ] [ gpe
    ]
    '''
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Get the interface index
        if phys_dev is not None:
            ifindex = ip_route.link_lookup(ifname=phys_dev)[0]
        else:
            ifindex = None
        # Create the link
        ip_route.link("add",
                      ifname=device,
                      kind="vxlan",
                      vxlan_id=vni,
                      vxlan_link=ifindex,
                      vxlan_group=remote,
                      vxlan_local=local,
                      vxlan_group6=remote6,
                      vxlan_local6=local6,
                      vxlan_ttl=ttl,
                      vxlan_tos=tos,
                      vxlan_label=flowlabel,
                      vxlan_port=dstport,
                      vxlan_port_range={
                          'low': srcport_min, 'high': srcport_max},
                      vxlan_learning=learning,
                      vxlan_proxy=proxy,
                      vxlan_rsc=rsc,
                      vxlan_l2miss=l2miss,
                      vxlan_l3miss=l3miss,
                      vxlan_udp_csum=udpcsum,
                      vxlan_udp_zero_csum6_tx=udp6zerocsumtx,
                      vxlan_udp_zero_csum6_rx=udp6zerocsumrx,
                      vxlan_ageing=ageing,
                      vxlan_gbp=gbp,
                      vxlan_gpe=gpe
                      )


def create_fdb_entry(dst, lladdr, dev, port=VXLAN_DSTPORT):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Replace the entry
        ip_route.fdb('add',
                     ifindex=ip_route.link_lookup(ifname=dev)[0],
                     lladdr=lladdr,
                     dst=dst,
                     port=port)


def update_fdb_entry(dst, lladdr, dev):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Replace the entry
        ip_route.fdb('replace',
                     ifindex=ip_route.link_lookup(ifname=dev)[0],
                     lladdr=lladdr,
                     dst=dst)


def remove_fdb_entry(lladdr, dev):
    # Get pyroute2 instance
    with IPRoute() as ip_route:
        # Remove the entry
        ip_route.fdb('del',
                     ifindex=ip_route.link_lookup(ifname=dev)[0],
                     lladdr=lladdr)


class TunnelVXLAN(tunnel_utils.TunnelMode):

    ''' VXLAN tunnel mode '''

    def __init__(self, name, priority, controller_ip,
                 ipv6_address_allocator, ipv4_address_allocator, debug=False):
        # VXLAN tunnel mode requires to exchange keep alive
        # messages to keep the tunnel open
        req_keep_alive_messages = True
        # NAT types supported by the VXLAN tunnel mode
        supported_nat_types = [
            pynat.OPEN,
            pynat.FULL_CONE,
            pynat.RESTRICTED_CONE,
            pynat.RESTRICTED_PORT
        ]
        # Set of initiated tenant IDs
        self.initiated = set()
        # Create tunnel mode
        super().__init__(name=name,
                         require_keep_alive_messages=req_keep_alive_messages,
                         supported_nat_types=supported_nat_types,
                         priority=priority,
                         controller_ip=controller_ip,
                         ipv6_net_allocator=None,
                         ipv4_net_allocator=None,
                         ipv6_address_allocator=ipv6_address_allocator,
                         ipv4_address_allocator=ipv4_address_allocator,
                         debug=debug)

    def create_tunnel_device_endpoint(self, tunnel_info):
        logging.info('Creating the VXLAN management interface')
        # Extract VXLAN port
        vxlan_port = tunnel_info.vxlan_port
        # VNI used for VXLAN management interface
        vni = MGMT_VNI
        # Create the VXLAN interface
        vxlan_name = '%s-%s' % (self.name, vni)
        logging.debug('Attempting to create the VXLAN '
                      'interface %s with VNI %s' % (vxlan_name, vni))
        create_vxlan(device=vxlan_name, vni=vni,
                     dstport=vxlan_port, srcport_min=vxlan_port,
                     srcport_max=vxlan_port+1, udpcsum=ENABLE_UDP_CSUM,
                     udp6zerocsumtx=ENABLE_UDP_CSUM,
                     udp6zerocsumrx=not ENABLE_UDP_CSUM,
                     learning=False)
        # Bring the interface UP
        logging.debug('Bringing the interface %s up' % vxlan_name)
        tunnel_utils.enable_interface(device=vxlan_name)
        # Include the VTEP's MAC address in the registration request
        # sent to the controller
        # The VTEP's MAC address is used to setup
        # the FDB entry on the controller side
        tunnel_info.device_vtep_mac = tunnel_utils.get_mac_address(
            ifname=vxlan_name)
        # Success
        logging.debug('The VXLAN interface has been created')
        return status_codes_pb2.STATUS_SUCCESS

    def create_tunnel_device_endpoint_end(self, tunnel_info):
        logging.info('Configuring the VXLAN management interface')
        # Extract the VTEP IPs, MAC and mask
        controller_vtep_ip = tunnel_info.controller_vtep_ip
        device_vtep_ip = tunnel_info.device_vtep_ip
        vtep_mask = tunnel_info.vtep_mask
        controller_vtep_mac = tunnel_info.controller_vtep_mac
        # VNI used for VXLAN management interface
        vni = MGMT_VNI
        # Add a private address to the interface
        vxlan_name = '%s-%s' % (self.name, vni)
        logging.debug('Attempting to assign the IP address %s/%s '
                      'to the VXLAN management interface %s'
                      % (device_vtep_ip, vtep_mask, vxlan_name))
        tunnel_utils.add_address(device=vxlan_name,
                                 address=device_vtep_ip,
                                 mask=vtep_mask)
        # Create a FDB entry that associate the VTEP MAC address
        # to the controller IP address
        logging.debug('Attempting to add the entry to the FDB\n'
                      'dst=%s, lladdr=%s, vxlan_name=%s'
                      % (self.controller_ip, controller_vtep_mac, vxlan_name))
        create_fdb_entry(dev=vxlan_name, lladdr=controller_vtep_mac,
                         dst=self.controller_ip)
        # Create a IP neighbor entry that associate the VTEP IP address
        # of the controller to the VTEP MAC address
        logging.debug('Attempting to add the neigh to the neigh table\n'
                      'dst=%s, lladdr=%s, vxlan_name=%s'
                      % (controller_vtep_ip, controller_vtep_mac, vxlan_name))
        tunnel_utils.create_ip_neigh(dev=vxlan_name,
                                     lladdr=controller_vtep_mac,
                                     dst=controller_vtep_ip)
        # Save the VTEP IP addresses
        self.controller_mgmtip = controller_vtep_ip
        self.device_vtep_ip = device_vtep_ip
        self.vtep_mask = vtep_mask
        # Success
        logging.debug('The VXLAN interface has been configured')
        return status_codes_pb2.STATUS_SUCCESS

    def create_tunnel_controller_endpoint(self, tunnel_info):
        logging.info('Configuring the VXLAN tunnel for the device %s'
                     % tunnel_info.device_id)
        # Extract the device ID
        device_id = tunnel_info.device_id
        # External IP and port of the device
        device_external_ip = tunnel_info.device_external_ip
        device_external_port = tunnel_info.device_external_port
        # MAC address of the VTEP's device
        device_vtep_mac = tunnel_info.device_vtep_mac
        # Extract the port
        port = tunnel_info.vxlan_port
        # Extract the tenant ID
        #tenantid = tunnel_info.tenantid
        tenantid = '0'
        # VNI used for VXLAN management interface
        vni = MGMT_VNI
        # Create the VXLAN interface
        vxlan_name = '%s-%s' % (self.name, vni)
        if tenantid not in self.initiated:
            # Initiate tenant ID
            self.initiated.add(tenantid)
            self.init_tenantid(tenantid)
            # Generate private addresses for the controller VTEP
            ip_mask = self.get_new_mgmt_ipv6(0, tenantid).split('/')
            controller_vtep_ipv6 = ip_mask[0]
            vtep_mask_ipv6 = int(ip_mask[1])
            ip_mask = self.get_new_mgmt_ipv4(0, tenantid).split('/')
            controller_vtep_ipv4 = ip_mask[0]
            vtep_mask_ipv4 = int(ip_mask[1])
            # Create the VXLAN interface
            logging.debug('First VXLAN tunnel, attempting to create '
                          'the VXLAN interface %s'
                          % vxlan_name)
            create_vxlan(device=vxlan_name, vni=vni,
                         # remote=device_external_ip,
                         local=self.controller_ip,
                         dstport=VXLAN_DSTPORT,
                         srcport_min=VXLAN_DSTPORT,
                         srcport_max=VXLAN_DSTPORT+1, udpcsum=ENABLE_UDP_CSUM,
                         udp6zerocsumtx=ENABLE_UDP_CSUM,
                         udp6zerocsumrx=not ENABLE_UDP_CSUM,
                         learning=False)
            # Bring the interface UP
            logging.debug('Bringing the interface %s up' % vxlan_name)
            tunnel_utils.enable_interface(device=vxlan_name)
            # Add a private address to the interface
            logging.debug('Attempting to assign the IP address %s/%s '
                          'to the VXLAN management interface %s'
                          % (controller_vtep_ipv6, vtep_mask_ipv6, vxlan_name))
            tunnel_utils.add_address(device=vxlan_name,
                                     address=controller_vtep_ipv6,
                                     mask=vtep_mask_ipv6)
            # Add a private address to the interface
            logging.debug('Attempting to assign the IP address %s/%s '
                          'to the VXLAN management interface %s'
                          % (controller_vtep_ipv4, vtep_mask_ipv4, vxlan_name))
            tunnel_utils.add_address(device=vxlan_name,
                                     address=controller_vtep_ipv4,
                                     mask=vtep_mask_ipv4)
        # Save the MAC address of the device's VTEP
        self.device_to_mac_addr[device_id] = device_vtep_mac
        # Generate private address for the device VTEP
        family = tunnel_utils.getAddressFamily(tunnel_info.device_external_ip)
        if family == socket.AF_INET6:
            ip_mask = self.get_new_mgmt_ipv6(0, tenantid).split('/')
            controller_vtep_ip = ip_mask[0]
            ip_mask = self.get_new_mgmt_ipv6(device_id, tenantid).split('/')
            device_vtep_ip = ip_mask[0]
            vtep_mask = int(ip_mask[1])
        elif family == socket.AF_INET:
            ip_mask = self.get_new_mgmt_ipv4(0, tenantid).split('/')
            controller_vtep_ip = ip_mask[0]
            ip_mask = self.get_new_mgmt_ipv4(device_id, tenantid).split('/')
            device_vtep_ip = ip_mask[0]
            vtep_mask = int(ip_mask[1])
        else:
            logging.error('Invalid family address: %s' %
                          tunnel_info.device_external_ip)
            return status_codes_pb2.STATUS_INTERNAL_ERROR
        # Create a FDB entry that associate the device VTEP MAC address
        # to the device IP address
        logging.debug('Attempting to add the entry to the FDB\n'
                      'dst=%s, lladdr=%s, vxlan_name=%s, port=%s'
                      % (device_external_ip, device_vtep_mac,
                         vxlan_name, device_external_port))
        create_fdb_entry(dev=vxlan_name, lladdr=device_vtep_mac,
                         dst=device_external_ip, port=port)
        # Create a IP neighbor entry that associate the VTEP IP address
        # of the device to the device VTEP MAC address
        logging.debug('Attempting to add the neigh to the neigh table\n'
                      'dst=%s, lladdr=%s, vxlan_name=%s'
                      % (device_vtep_ip, device_vtep_mac, vxlan_name))
        tunnel_utils.create_ip_neigh(dev=vxlan_name, lladdr=device_vtep_mac,
                                     dst=device_vtep_ip)
        # Update and return the tunnel info
        tunnel_info.controller_vtep_mac = tunnel_utils.get_mac_address(
            ifname=vxlan_name)
        tunnel_info.controller_vtep_ip = controller_vtep_ip
        tunnel_info.device_vtep_ip = device_vtep_ip
        tunnel_info.vtep_mask = vtep_mask
        # Success
        logging.debug('The VXLAN interface has been configured')
        return status_codes_pb2.STATUS_SUCCESS

    def destroy_tunnel_device_endpoint(self, tunnel_info):
        logging.info('Destroying the VXLAN tunnel')
        # VNI used for VXLAN management interface
        vni = MGMT_VNI
        # Name of the VXLAN interface
        vxlan_name = '%s-%s' % (self.name, vni)
        # Remove the IP address assigned to the interface
        device_vtep_ip = self.device_vtep_ip
        vtep_mask = self.vtep_mask
        logging.debug('Attempting to remove the IP address %s/%s '
                      'from the VXLAN management interface %s'
                      % (device_vtep_ip, vtep_mask, vxlan_name))
        tunnel_utils.del_address(device=vxlan_name,
                                 address=device_vtep_ip,
                                 mask=vtep_mask)
        # Delete the VXLAN interface
        logging.debug('Attempting to remove the VXLAN '
                      'interface %s' % vxlan_name)
        tunnel_utils.delete_interface(device=vxlan_name)
        # Remove the VTEP IP addresses
        self.controller_mgmtip = None
        self.device_vtep_ip = None
        self.device_vtep_mask = None
        # Success
        logging.debug('The VXLAN interface has been removed')
        return status_codes_pb2.STATUS_SUCCESS

    def destroy_tunnel_controller_endpoint(self, tunnel_info):
        logging.info('Destroying the VXLAN tunnel for the device %s'
                     % tunnel_info.device_id)
        # Extract the device ID
        device_id = tunnel_info.device_id
        # Extract the tenant ID
        #tenantid = tunnel_info.tenantid
        tenantid = '0'
        # VNI used for VXLAN management interface
        vni = MGMT_VNI
        # Name of the VXLAN interface
        vxlan_name = '%s-%s' % (self.name, vni)
        # Remove the IP neighbor entry that associate the VTEP IP address
        # of the device to the device VTEP MAC address
        device_vtep_ip = self.get_device_mgmtip(
            tenantid, device_id).split('/')[0]
        logging.debug('Attempting to remove the neigh from the neigh table\n'
                      'dst=%s, vxlan_name=%s'
                      % (device_vtep_ip, vxlan_name))
        tunnel_utils.remove_ip_neigh(dev=vxlan_name,
                                     dst=device_vtep_ip)
        # Remove the FDB entry that associate the device VTEP MAC address
        # to the device IP address
        device_vtep_mac = self.get_device_vtep_mac(device_id)
        logging.debug('Attempting to remove the entry from the FDB\n'
                      'lladdr=%s, vxlan_name=%s'
                      % (device_vtep_mac, vxlan_name))
        remove_fdb_entry(dev=vxlan_name, lladdr=device_vtep_mac)
        # Release the private IP address associated to the device
        self.release_ipv4_address(device_id, tenantid)
        self.release_ipv6_address(device_id, tenantid)
        # If the tenant has no more devices, we can de-allocate the VTEP
        if self.num_addresses(tenantid) == 0:
            # Remove the address from the VTEP interface
            ip_mask = self.get_device_mgmtipv4(tenantid, 0).split('/')
            controller_vtep_ipv4 = ip_mask[0]
            vtep_mask_ipv4 = ip_mask[1]
            logging.debug('Attempting to remove the IP address %s/%s '
                          'from the VXLAN management interface %s'
                          % (controller_vtep_ipv4, vtep_mask_ipv4, vxlan_name))
            tunnel_utils.del_address(device=vxlan_name,
                                     address=controller_vtep_ipv4,
                                     mask=vtep_mask_ipv4)
            # Remove the address from the VTEP interface
            ip_mask = self.get_device_mgmtipv6(tenantid, 0).split('/')
            controller_vtep_ipv6 = ip_mask[0]
            vtep_mask_ipv6 = ip_mask[1]
            logging.debug('Attempting to remove the IP address %s/%s '
                          'from the VXLAN management interface %s'
                          % (controller_vtep_ipv6, vtep_mask_ipv6, vxlan_name))
            tunnel_utils.del_address(device=vxlan_name,
                                     address=controller_vtep_ipv6,
                                     mask=vtep_mask_ipv6)
            # Remove the VXLAN interface
            logging.debug('Last VXLAN tunnel, attempting to remove '
                          'the VXLAN interface %s'
                          % vxlan_name)
            tunnel_utils.remove_interface(device=vxlan_name)
            # Destroy tenant ID
            self.release_tenantid(tenantid)
            self.initiated.remove(tenantid)
        # Success
        logging.debug('The VXLAN interface has been removed')
        return status_codes_pb2.STATUS_SUCCESS

    def update_tunnel_device_endpoint(self, tunnel_info):
        res = self.destroy_tunnel_device_endpoint(tunnel_info)
        if res == status_codes_pb2.STATUS_SUCCESS:
            res = self.create_tunnel_device_endpoint(tunnel_info)
        return res

    def update_tunnel_device_endpoint_end(self, tunnel_info):
        return self.create_tunnel_device_endpoint_end(tunnel_info)

    def update_tunnel_controller_endpoint(self, tunnel_info):
        res = self.destroy_tunnel_controller_endpoint(tunnel_info)
        if res == status_codes_pb2.STATUS_SUCCESS:
            res = self.create_tunnel_controller_endpoint(tunnel_info)
        return res

    # Return the private IPv6 of the device
    def get_device_mgmtipv6(self, tenantid, device_id):
        if tenantid not in self.device_to_mgmt_ipv6:
            return None
        return self.device_to_mgmt_ipv6[tenantid].get(device_id)

    # Return the private IPv4 of the device
    def get_device_mgmtipv4(self, tenantid, device_id):
        if tenantid not in self.device_to_mgmt_ipv4:
            return None
        return self.device_to_mgmt_ipv4[tenantid].get(device_id)

    # Return the private IP of the device
    def get_device_mgmtip(self, tenantid, device_id):
        tenantid = '0'
        addr = self.get_device_mgmtipv4(tenantid, device_id)
        if addr is None:
            addr = self.get_device_mgmtipv6(tenantid, device_id)
        return addr
