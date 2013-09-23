#!/bin/bash

# process file into "word,definition" lines
awk -f synsets.awk synsets.txt > synsets_out_shell.txt

# count unique words
cat synsets_out_shell.txt | cut -d',' -f1 | sort -u | wc -l

# process file into "country,year,place" lines
cat worldcup.txt | sed 's/\[\[\([0-9]*\)[^]]*\]\]/\1/g; s/.*fb|\([A-Za-z]*\)}}/\1/g; s/<sup><\/sup>//g; s/|bgcolor[^|]*//g; s/|align=center[^|]*//g' | grep -v "^[!:]" | awk -f worldcup.awk > worldcup_out_shell.txt
