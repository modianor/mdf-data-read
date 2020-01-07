# *_*coding:utf-8 *_*
#
import csv
from typing import List

from asammdf import MDF3
from asammdf.blocks.utils import Group
from asammdf.blocks.v2_v3_blocks import Channel, DataGroup

dg_data_start = None
cc_conversion_start = None
record_id = None
bit_count = None
conversion_a = None
channel_name = 'EnSpdHSC2:CAN2'
target_data_dir = 'data/target.dat'
clean_data_dir = 'data/clean.csv'
orgin_data_dir = 'data/rec_2018-07-05-13_00_09.dat'

dataset: MDF3 = MDF3(name=orgin_data_dir, version='3.30')
groups = dataset.groups
for i, group in enumerate(groups):
    g: Group = group
    data_group: DataGroup = g.data_group
    channels: List = g.channels
    for j, channel in enumerate(channels):
        c: Channel = channel
        if c.name == channel_name:
            record_id = i
            dg_data_start = data_group.data_block_addr
            conversion_a = c.conversion.a
            cc_conversion_start = c.conversion_addr
            bit_count = c.bit_count
            print(
                'record_id = {} , dg_data_start = {} , bit_count = {} , cc_conversion_start = {} , conversion_a = {}'.format(
                    hex(record_id),
                    hex(dg_data_start),
                    bit_count,
                    hex(cc_conversion_start),
                    conversion_a))

with open('data/target.dat', 'wb+') as w:
    with open(orgin_data_dir, 'rb') as r:
        r.seek(dg_data_start, 0)
        while True:
            data = r.read(bit_count)
            if not data:
                break
            record_id_bin = data[:2]
            if record_id_bin == record_id.to_bytes(length=2, byteorder='little', signed=False):
                w.write(data)
print('............完成提取{}数据............'.format(channel_name))

with open(clean_data_dir, 'w+', encoding='utf-8') as w:
    csv_writer = csv.writer(w)
    csv_writer.writerow(["Record_Id", "Time_Stamp", "Ensp"])

    with open(target_data_dir, 'rb') as r:
        while True:
            data = r.read(16)
            if not data:
                break
            record_id_bin = data[:3][:-1]
            time_stamp_bin = data[2:9][:-1]
            ensp_bin = data[9:12][:-1]
            record_id = int.from_bytes(record_id_bin, byteorder='little', signed=False)
            time_stamp = int.from_bytes(time_stamp_bin, byteorder='little', signed=False)
            ensp = int.from_bytes(ensp_bin, byteorder='big', signed=False) * conversion_a
            csv_writer.writerow([record_id, time_stamp, ensp])
            # print(record_id, str(time_stamp) + '微秒', ensp)
print('............完成解析{}数据............'.format(channel_name))
