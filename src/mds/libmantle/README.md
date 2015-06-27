#Mantle - a framework for injectable metadata balancers

#Build Prerequisites
Mantle depends in Lua:

`sudo apt-get install lua liblua5.2-dev`

#Operation
Point Mantle at the library directory:

`ceph --admin-daemon ...asok config set mds_bal_dir ".../ceph/src/mds/balancers/"`

Set values for all policies:

`ceph --admin-daemon ...asok config set mds_bal_metaload    "IWR"`
`ceph --admin-daemon ...asok config set mds_bal_mdsload     "MDSs[i]["cpu"]"`
`ceph --admin-daemon ...asok config set mds_bal_when        "if MDSs[whoami]["cpu"] > 0.8 then"`
`ceph --admin-daemon ...asok config set mds_bal_where       "for i=1,#targets do targets[i] = 0 end``"`




