#!/bin/bash          
cd /home/app/rant
pyenv activate eip
python get_comments_remote.py > log_file 2> err_file

        
