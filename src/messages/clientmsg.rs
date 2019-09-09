use crate::task::TaskSpec;
use serde::Deserialize;
use std::collections::HashMap;
/*

[{b'op': b'update-graph',
  b'tasks': {b'do_something-1af9b5ab-7231-4fcf-8635-7ae9d5ccafb0': {b'function': b'\x80\x04\x95\\\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x02KCC\x08|\x00d\x01\x14\x00S\x00\x94NK\n\x86\x94)\x8c\x01x\x94\x85\x94\x8c\tclient.py\x94\x8c\x0cdo_something\x94K\x07C\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x16\x8c\x04name\x94h\x0f\x8c\x03doc\x94N\x8c\x08qualname\x94h\x0futR.',
    b'args': b'\x80\x04\x95\x05\x00\x00\x00\x00\x00\x00\x00K\n\x85\x94.'}},
  b'dependencies': {b'do_something-1af9b5ab-7231-4fcf-8635-7ae9d5ccafb0': []},
  b'keys': [b'do_something-1af9b5ab-7231-4fcf-8635-7ae9d5ccafb0'],
  b'restrictions': {},
  b'loose_restrictions': None,
  b'priority': {b'do_something-1af9b5ab-7231-4fcf-8635-7ae9d5ccafb0': 0},
  b'user_priority': 0,
  b'resources': None,
  b'submitting_task': None,
  b'retries': None,
  b'fifo_timeout': b'60s',
  b'actors': None}]

  {b'op': b'update-graph',
 b'tasks': {b'adder-deb6ad6a-0aa5-4c17-a00c-74de841faf9d': {b'function': b'\x80\x04\x95W\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x02K\x00K\x02K\x02KCC\x08|\x00|\x01\x17\x00S\x00\x94N\x85\x94)\x8c\x01x\x94\x8c\x01y\x94\x86\x94\x8c\tclient.py\x94\x8c\x05adder\x94K\x0bC\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x17\x8c\x04name\x94h\x10\x8c\x03doc\x94N\x8c\x08qualname\x94h\x10utR.',
   b'args': b'\x80\x04\x959\x00\x00\x00\x00\x00\x00\x00\x8c1do_something-f7f4a7a2-cd7d-43ae-8f87-524fc80aa4e9\x94K\x03\x86\x94.'},
  b'do_something-f7f4a7a2-cd7d-43ae-8f87-524fc80aa4e9': {b'function': b'\x80\x04\x95\\\x01\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x94\x93\x94(h\x00\x8c\x0f_make_skel_func\x94\x93\x94h\x00\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x02KCC\x08|\x00d\x01\x14\x00S\x00\x94NK\n\x86\x94)\x8c\x01x\x94\x85\x94\x8c\tclient.py\x94\x8c\x0cdo_something\x94K\x07C\x02\x00\x02\x94))t\x94R\x94J\xff\xff\xff\xff}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94\x8c\x08__file__\x94\x8c\tclient.py\x94u\x87\x94R\x94}\x94(\x8c\x07globals\x94}\x94\x8c\x08defaults\x94N\x8c\x04dict\x94}\x94\x8c\x0eclosure_values\x94N\x8c\x06module\x94h\x16\x8c\x04name\x94h\x0f\x8c\x03doc\x94N\x8c\x08qualname\x94h\x0futR.',
   b'args': b'\x80\x04\x95\x05\x00\x00\x00\x00\x00\x00\x00K\n\x85\x94.'}},
 b'dependencies': {b'adder-deb6ad6a-0aa5-4c17-a00c-74de841faf9d': [b'do_something-f7f4a7a2-cd7d-43ae-8f87-524fc80aa4e9'],
  b'do_something-f7f4a7a2-cd7d-43ae-8f87-524fc80aa4e9': []},
 b'keys': [b'adder-deb6ad6a-0aa5-4c17-a00c-74de841faf9d'],
 b'restrictions': {},
 b'loose_restrictions': None,
 b'priority': {b'do_something-f7f4a7a2-cd7d-43ae-8f87-524fc80aa4e9': 0,
  b'adder-deb6ad6a-0aa5-4c17-a00c-74de841faf9d': 1},
 b'user_priority': 0,
 b'resources': None,
 b'submitting_task': None,
 b'retries': None,
 b'fifo_timeout': b'60s',
 b'actors': None}

*/

#[derive(Deserialize, Debug)]
pub struct UpdateGraphMsg {
    pub tasks: HashMap<String, TaskSpec>,
    pub dependencies: HashMap<String, Vec<String>>,
    pub keys: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct ClientReleasesKeysMsg {
    pub keys: Vec<String>,
    pub client: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "kebab-case")]
pub enum ClientMessage {
    HeartbeatClient,
    UpdateGraph(UpdateGraphMsg),
    ClientReleasesKeys(ClientReleasesKeysMsg),
}
