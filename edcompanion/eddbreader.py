#pylint: disable=missing-module-docstring
#pylint: disable=missing-function-docstring
#pylint: disable=invalid-name

import sys
import time
from pathlib import Path
import gzip
import json

def edc_dbfilereader(filename, verbose=False):
    """
        Opens 'filename' as generator for eddb style objects
    """

    filesize=Path(filename).stat().st_size
    chunksize = 64 * 1024 * 1024
    est_count = int(8*filesize/chunksize) + 1
    print(f"Reading {filename}, {round(filesize/(1024*1024),1)} Mb in approx {est_count} chunks")

    count = 0
    system_count = 0
    item = None

    start = time.process_time()
    #try:

    with gzip.open(filename, 'rt') as jsonfile:
        firstline = jsonfile.readline()

        while True:
            count += 1
            chunk = jsonfile.readlines(chunksize)
            if chunk:
                for line in chunk:
                    if len(line) < 5:
                        continue

                    yield json.loads(line.rstrip(',\n\r '))

                    system_count += 1

                yield {}

                sys.stdout.write(f"\r{count}/{est_count}\t{100*count/est_count:3.2f}%, {int(system_count / (time.process_time() - start)):6} /s, {system_count:9} systems, {((est_count - count) * (time.process_time() - start)/count):5.1f} seconds remaining")

            else:
                print(f"\nEmpty chunk -> Done! Imported {system_count} systems in {round(time.process_time() - start,1)} seconds")
                break

    tpl = (time.process_time() - start)/system_count
    sys.stdout.write(f"\n{ (time.process_time() - start)} seconds {system_count} systems, per system {round(1000000*tpl,2)} us")


def edc_dbfile_process(filename, process_chunk, verbose=False):
    """Opens file and calls process_chunk to process batches of items"""
    filesize=Path(filename).stat().st_size
    chunksize = 64 * 1024 * 1024
    est_count = int(8*filesize/chunksize) + 1
    print(f"Reading {filename}, {round(filesize/(1024*1024),1)} Mb in approx {est_count} chunks")

    count = 0
    system_count = 0

    start = time.process_time()
    with gzip.open(filename, 'rt') as jsonfile:

        while True:
            count += 1
            chunk = jsonfile.readlines(chunksize)
            if chunk:
                data = []
                for line in chunk:
                    if len(line) < 5:
                        continue

                    item = json.loads(line[0:-2]) if line[-2] == "," else json.loads(line)
                    data.append(item)
                    system_count += 1

                process_chunk(data)
                sys.stdout.write(f"\r{count}/{est_count}\t{100*count/est_count:3.2f}%, {int(system_count / (time.process_time() - start)):6} /s, {system_count:9} systems, {((est_count - count) * (time.process_time() - start)/count):5.1f} seconds remaining")

            else:
                print(f"\nEmpty chunk -> Done! Imported {system_count} systems in {round(time.process_time() - start,1)} seconds")
                break




    tpl = (time.process_time() - start)/system_count
    sys.stdout.write(f"\n{ (time.process_time() - start)} seconds {system_count} systems, per system {round(1000000*tpl,2)} us")


