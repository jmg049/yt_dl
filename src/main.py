import logging
import argparse
import os
import subprocess
import sys

import pandas as pd
from tqdm import tqdm
from tqdm_multiprocess.logger import setup_logger_tqdm
from tqdm_multiprocess import TqdmMultiProcessPool

logger = logging.getLogger(__name__)


def populate_commands(data, output_dir, mode) -> str:
    AUDIO_COMMAND_TEMPLATE = 'yt-dlp --force-keyframes-at-cuts --restrict-filenames -q --verbose --no-warnings --retries 50 --fragment-retries 50 {url} --download-sections "*{start_time}-{end_time}" -f bestaudio[ext=wav]/bestaudio[ext=webm]/bestaudio -o "{output_dir}/{f_name}.%(ext)s" "{yt_id}"'
    COMMAND_TEMPLATE = 'yt-dlp --force-keyframes-at-cuts --restrict-filenames  -q --verbose --no-warnings --retries 50 {url} --download-sections "*{start_time}-{end_time}" -f bestvideo[ext=mp4][protocol^=http] -o "{output_dir}/{f_name}.%(ext)s" "{yt_id}"'
    BOTH_COMMAND_TEMPLATE = 'yt-dlp --force-keyframes-at-cuts --restrict-filenames  -q --verbose --no-warnings --retries 50 {url} --download-sections "*{start_time}-{end_time}" -o "{output_dir}/{f_name}.%(ext)s" "{yt_id}"'

    YT_URL = 'https://www.youtube.com/watch?v={video_id}'
    def create_command(row):
        url = YT_URL.format(video_id=row['yt_id'])
        start_time = row['start_seconds']
        end_time = row.get('end_seconds', str(int(start_time)+10))
        f_name = row['yt_id'].replace('\n', '')

        match mode :
            case 'audio' :
                out= f_name, AUDIO_COMMAND_TEMPLATE.format(url=url, start_time=start_time, end_time=end_time, output_dir=output_dir, f_name=f_name, yt_id=url)
                return out
            case 'video' :
                return f_name, COMMAND_TEMPLATE.format(url=url, start_time=start_time, end_time=end_time, output_dir=output_dir, f_name=f_name, yt_id=url)
            case 'both' :
                return f_name, BOTH_COMMAND_TEMPLATE.format(url=url, start_time=start_time, end_time=end_time, output_dir=output_dir, f_name=f_name, yt_id=url)

    commands = data.apply(lambda row: create_command(row), axis=1)
    return commands


def run_command(command, total, mode, tqdm_func, global_tqdm):
    match mode:
        case 'audio':
            # try:
            yt_id = command[command.rindex('-o')+4:]
        case 'video':
            yt_id = command[command.rindex('-o')+4:]
        case 'both':
            yt_id = command[command.rindex('-o')+4:]    
    process_output = subprocess.run(command, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(process_output.stdout)
    logger.error(process_output.stderr)
    if process_output.returncode != 0:
        logger.error("Failed to download: {}".format(yt_id))

    global_tqdm.update()
    return yt_id, process_output.returncode

def process_data(commands, n_procs, mode):
    pool = TqdmMultiProcessPool(n_procs)
    total = len(commands)
    tasks = [(run_command, (command,total, mode)) for command in commands]


    with tqdm(total=total, leave=True, position=0) as global_pb:
        global_pb.set_description('Downloading')
        result = pool.map(global_tqdm=global_pb, tasks=tasks, on_done=lambda _: (), on_error=lambda _: ()),
    return result


def main(input_csv, output_dir, n_procs, mode='audio'):   
    data = pd.read_csv(input_csv)
    data = data.iloc[::-1]
    print(os.listdir(output_dir))
    existing_files = os.listdir(output_dir)
    existing_files = [f.split('.')[0] for f in existing_files]
    data = data[~data['yt_id'].isin(existing_files)]
    print(data)
    
    yt_dlp_commands = populate_commands(data, output_dir, mode)
    print(len(yt_dlp_commands))
    yt_ids = [c[0] for c in yt_dlp_commands]
    commands = [c[1] for c in yt_dlp_commands]    
    

    # filtered_commands = [] 
    # for yt_id, command in zip(yt_ids, commands):
    #     if yt_id not in existing_files:
    #         filtered_commands.append(command)


    print("Existing files: ", len(existing_files))
    print("Total expected files: ", len(yt_ids))
    _ = process_data(commands, n_procs, mode)

    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_csv', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--n_procs', type=int, required=False, default=1)
    parser.add_argument('--mode', type=str, required=False, default='audio')
    parser.add_argument('--log_file', type=str, required=False, default='log.log')
    args = parser.parse_args()
    setup_logger_tqdm(args.log_file)
    os.makedirs(args.output_dir, exist_ok=True)
    main(args.input_csv, args.output_dir, args.n_procs, args.mode)
