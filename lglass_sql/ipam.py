# coding: utf-8

import lglass.ipam

import lglass_sql.nic

class IPAMDatabase(lglass_sql.nic.NicDatabase,
        lglass.ipam.IPAMDatabaseMixin):
    def __init__(self, *args, **kwargs):
        lglass_sql.nic.NicDatabase.__init__(self, *args, **kwargs)
        lglass.ipam.IPAMDatabaseMixin.__init__(self)
        self.inverse_keys |= {"hostname", "vlan-id", "net",
                "l2-address", "vxlan-vni"}

