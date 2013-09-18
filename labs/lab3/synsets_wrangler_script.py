from wrangler import dw
import sys

if(len(sys.argv) < 3):
  sys.exit('Error: Please include an input and output file.  Example python script.py input.csv output.csv')

w = dw.DataWrangler()

# Split data repeatedly on newline  into  rows
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on="\n",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character=None))

# Split data repeatedly on ','
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=",",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character="\""))

# Cut  on '"'
w.add(dw.Cut(column=[],
             table=0,
             status="active",
             drop=False,
             result="column",
             update=True,
             insert_position="right",
             row=None,
             on="\"",
             before=None,
             after=None,
             ignore_between=None,
             which=1,
             max=0,
             positions=None))

# Split split1 repeatedly on ' '  into  rows
w.add(dw.Split(column=["split1"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on=" ",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max="0",
               positions=None,
               quote_character=None))

# Split split2 repeatedly on ';'  into  rows
w.add(dw.Split(column=["split2"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on=";",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max="0",
               positions=None,
               quote_character=None))

# Drop split
w.add(dw.Drop(column=["split"],
              table=0,
              status="active",
              drop=True))

w.apply_to_file(sys.argv[1]).print_csv(sys.argv[2])
