PL/SQL Developer Test script 3.0
54
declare 
  c clob;
  class_name varchar2(200);
  method_name varchar2(200);

  function get_part(class_name varchar2,method_name varchar2,oper_type varchar2)return clob
  is
    out_clob clob;
  begin
    for r in (select text
              from sources 
              where name = (select m.id from METHODS m where m.class_id = class_name and m.short_name = method_name)
                and type = oper_type order by line)
    loop
      out_clob := out_clob || r.text || chr(10);
    end loop;
    return out_clob;
  end;
  
  /*function cpad(text varchar2,n integer,c char)return varchar2
  is
  begin
    return LPAD(RPAD(text,LENGTH(text) + (n - LENGTH(text)) / 2,c),n,c);
  end;*/
  
  /*function get_with_header(class_name varchar2,method_name varchar2,oper_type varchar2)return clob
  is
    --out_clob clob;
    d varchar2(2000);
  begin
    d := '';
    --d := d || chr(10) || cpad('-',50,'-');
    --d := d || chr(10) || '--' || cpad(oper_type,46,' ') || '--';
    --d := d || chr(10) || cpad('-',50,'-') || chr(10);
    d := d || chr(10) || '';
    d := d || chr(10) || '' || cpad(oper_type,48,' ') || '';
    d := d || chr(10) || '';
    d := d || chr(10) ||  rtrim(get_part(class_name,method_name,oper_type),chr(10));
    d := d || chr(10) || '';
    return d;
    --return rtrim(d || get_part(class_name,method_name,oper_type),chr(10));
  end;*/
  
  
begin
  class_name := :class_name;
  method_name := :method_name;
  :body := get_part(class_name,method_name,'EXECUTE');
  :validate := get_part(class_name,method_name,'VALIDATE');
  :globals := get_part(class_name,method_name,'PUBLIC');
  :locals := get_part(class_name,method_name,'PRIVATE');
  :script := get_part(class_name,method_name,'VBSCRIPT');

end;
14
class_name
1
BRK_MSG
5
method_name
1
POST_PROC_LIB
5
oper_type
1
EXECUTE
-5
out
1
<CLOB>
-112
e
1
<CLOB>
-112
v
1
<CLOB>
-112
g
1
<CLOB>
-112
l
1
<CLOB>
-112
s
1
<CLOB>
-112
body
0
5
validate
0
5
globals
29

function  ObrProv(doc_ref ref [MAIN_DOCUM] default null) return CLOB;
	-- ab-518  -- сделать вызов этой функции в учете проценов в зоде - нужно передать док причисления и не brk_msg

--  процедуры для запуска в джобах:

-- смена транзакционныой схеме пластика по факту смены документом статуса TO_KART
procedure ObrKartEvents; -- не используется но можно запустить -- чисто по докам без использования brk_msg смена транзакционных схем картам при возникновении/гашении картотеки

-- постобработка проводок
procedure ObrProvEvents; --  -- не используется с аб-1611 но можно запустить чисто пр докам без использования brk_msg
procedure ObrProvEventsMult(StreamNumber integer,StreamCount integer); -- ab-1611

function GetStartDateTime  return date;
PRAGMA RESTRICT_REFERENCES ( GetStartDateTime, WNDS );

function IsTrn(hist_state [HISTORY_STAT_ARR], iddoc number) return number;
PRAGMA RESTRICT_REFERENCES ( IsTrn, WNDS );

function IsPST(hist_state [HISTORY_STAT_ARR]) return number;
PRAGMA RESTRICT_REFERENCES ( IsPST, WNDS );








5
locals
0
5
script
0
5
0
