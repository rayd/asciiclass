from wrangler import dw

# Synsets script

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

w.apply_to_file("synsets.txt").print_csv("synsets_out.txt")

# Worldcup script

w = dw.DataWrangler()

# Split data repeatedly on '|-'  into  rows
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on="\\|-",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max="0",
               positions=None,
               quote_character=None))

# Extract from data between 'fb|' and '}}'
w.add(dw.Extract(column=["data"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="}}",
                 after="fb\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Split data repeatedly on newline
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on="\n",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max="0",
               positions=None,
               quote_character=None))

# Drop split, split1, split6, split7
w.add(dw.Drop(column=["split","split1","split6","split7"],
              table=0,
              status="active",
              drop=True))

# Edit split2 row 1  to ' 1 '
w.add(dw.Edit(column=["split2"],
              table=0,
              status="active",
              drop=False,
              result="column",
              update=True,
              insert_position="right",
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.RowIndex(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  indices=[0])]),
              on=None,
              before=None,
              after=None,
              ignore_between=None,
              which=1,
              max=1,
              positions=None,
              to="1",
              update_method=None))

# Edit split4 row 1  to '  '
w.add(dw.Edit(column=["split4"],
              table=0,
              status="active",
              drop=False,
              result="column",
              update=True,
              insert_position="right",
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.RowIndex(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  indices=[0])]),
              on=None,
              before=None,
              after=None,
              ignore_between=None,
              which=1,
              max=1,
              positions=None,
              to="",
              update_method=None))

# Edit split3 row 1  to ' 2 '
w.add(dw.Edit(column=["split3"],
              table=0,
              status="active",
              drop=False,
              result="column",
              update=True,
              insert_position="right",
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.RowIndex(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  indices=[0])]),
              on=None,
              before=None,
              after=None,
              ignore_between=None,
              which=1,
              max=1,
              positions=None,
              to="2",
              update_method=None))

# Edit split4 row 1  to ' 3 '
w.add(dw.Edit(column=["split4"],
              table=0,
              status="active",
              drop=False,
              result="column",
              update=True,
              insert_position="right",
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.RowIndex(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  indices=[0])]),
              on=None,
              before=None,
              after=None,
              ignore_between=None,
              which=1,
              max=1,
              positions=None,
              to="3",
              update_method=None))

# Edit split5 row 1  to ' 4 '
w.add(dw.Edit(column=["split5"],
              table=0,
              status="active",
              drop=False,
              result="column",
              update=True,
              insert_position="right",
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.RowIndex(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  indices=[0])]),
              on=None,
              before=None,
              after=None,
              ignore_between=None,
              which=1,
              max=1,
              positions=None,
              to="4",
              update_method=None))

# Fold split2, split3, split4, split5  using  1 as a key
w.add(dw.Fold(column=["split2","split3","split4","split5"],
              table=0,
              status="active",
              drop=False,
              keys=[0]))

# Split value repeatedly on ','
w.add(dw.Split(column=["value"],
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
               max="0",
               positions=None,
               quote_character=None))

# Extract from split between 'Cup|' and ']'
w.add(dw.Extract(column=["split"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="Cup\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from split8 between 'Cup|' and ']'
w.add(dw.Extract(column=["split1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="Cup\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from split2 between 'Cup|' and ']'
w.add(dw.Extract(column=["split2"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="Cup\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from split3 between 'Cup|' and ']'
w.add(dw.Extract(column=["split3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="Cup\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from split4 between 'Cup|' and ']'
w.add(dw.Extract(column=["split4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="Cup\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Drop split, split1, split2, split3...
w.add(dw.Drop(column=["split","split1","split2","split3","split4"],
              table=0,
              status="active",
              drop=True))

# Cut from extract3 on '*'
w.add(dw.Cut(column=["extract3"],
             table=0,
             status="active",
             drop=False,
             result="column",
             update=True,
             insert_position="right",
             row=None,
             on="\\*",
             before=None,
             after=None,
             ignore_between=None,
             which=1,
             max=1,
             positions=None))

# Delete rows where extract1 is null
w.add(dw.Filter(column=[],
                table=0,
                status="active",
                drop=False,
                row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.IsNull(column=[],
                table=0,
                status="active",
                drop=False,
                lcol="extract1",
                value=None,
                op_str="is null")])))

# Fold extract1, extract2, extract3, extract4...  using  header as a key
w.add(dw.Fold(column=["extract1","extract2","extract3","extract4","extract5"],
              table=0,
              status="active",
              drop=False,
              keys=[-1]))

# Drop fold1
w.add(dw.Drop(column=["fold1"],
              table=0,
              status="active",
              drop=True))

# Delete  rows where value is null
w.add(dw.Filter(column=[],
                table=0,
                status="active",
                drop=False,
                row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.IsNull(column=[],
                table=0,
                status="active",
                drop=False,
                lcol="value",
                value=None,
                op_str="is null")])))

w.apply_to_file("worldcup.txt").print_csv("worldcup_out.txt")
