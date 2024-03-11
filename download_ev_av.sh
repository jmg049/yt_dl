#! /bin/bash
python3 src/main.py --input_csv "./ev_av_base.csv" --output_dir "/home/jmg/code/datasets/ev_av/audio" --n_procs 16 --mode "both" --log_file "./logs/download_ev_av_audio.log"
