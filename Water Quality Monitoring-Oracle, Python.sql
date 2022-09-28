Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2001";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2002";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2003";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2004";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2005";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2006";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2007";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2008";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2009";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2010";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2011";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2012";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2013";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2014";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2015";

Insert Into WATERQUALITY_STAGINGAREA 
Select*From "2016";

delete from WATERQUALITY_STAGINGAREA where "determinandlabel" = 'NO FLOW/SAMP';


Create table Time_Dim(
TIME_ID VARCHAR2(255 BYTE),
primary key(TIME_ID),
Dates varchar(20),
Day varchar(8),
Week_of_Year number(5,0),
Month number(3,0),
Month_Name varchar(9),
Year varchar(4));

declare
Cursor ws is
select distinct "samplesampleDateTime" as d from waterquality_stagingarea;
begin
for w in ws loop
insert into Time_Dim values(w.d,
TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD')),
TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD'), 'DD'),
to_number(TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD'), 'IW')),
to_number(TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD'), 'MM')),
TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD'), 'Month'),
TO_CHAR(to_date(substr(w.d,0,10), 'YYYY.MM.DD'), 'YYYY'));
end loop;
end;
/

create table Location_Dim(
Location_ID VARCHAR2(255 BYTE),
Location VARCHAR2(255 BYTE),
Primary Key(Location_ID));


INSERT INTO Location_Dim (Location_ID, Location)
SELECT distinct "samplesamplingPointnotation", "samplesamplingPointlabel"
FROM waterquality_stagingarea;


create table Measurement_Dim(
WSensor_ID number,
Definition VARCHAR2(255 BYTE),
Unit varchar2(255 byte),
Type VARCHAR2(255 BYTE),
Primary Key(WSensor_ID));

INSERT INTO measurement_dim(wsensor_id, Definition,Unit, Type)
SELECT distinct  "determinandnotation","determinanddefinition","determinandunitlabel","determinandlabel"
FROM waterquality_stagingarea


CREATE TABLE Fact_Table
(WSensor_ID number,
Location_ID VARCHAR2(255 BYTE),
TIME_ID VARCHAR2(255 BYTE),
Result BINARY_DOUBLE,
NB_Meaesurements number,
CONSTRAINT WSensor_ID_FK FOREIGN KEY (WSensor_ID) REFERENCES Measurement_Dim(WSensor_ID) ON DELETE CASCADE,
CONSTRAINT Location_ID_FK FOREIGN KEY (Location_ID) REFERENCES location_Dim(Location_ID) ON DELETE CASCADE,
CONSTRAINT TIME_ID_FK FOREIGN KEY (TIME_ID) REFERENCES TIME_DIM(TIME_ID) ON DELETE CASCADE,
CONSTRAINT Fact_Table_PK PRIMARY KEY (WSensor_ID, Location_ID, TIME_ID));


DECLARE
CURSOR factData IS
SELECt distinct md. WSENSOR_ID,ld.Location_ID,td.TIME_ID,ws."result",
COUNT(*) AS NB_Meaesurements
from measurement_dim md,location_dim ld,time_dim td,waterquality_stagingarea ws
where ws."determinandnotation"= md.wsensor_id AND ws."samplesamplingPointnotation"=ld.location_id and ws."samplesampleDateTime" = td.time_id
GROUP BY md.WSENSOR_ID,ld.Location_ID,td.TIME_ID,ws."result";
BEGIN
FOR fd IN factData
LOOP
    INSERT INTO fact_table VALUES(fd.WSensor_ID,fd.Location_ID,fd.TIME_ID,fd."result",fd.NB_Meaesurements);
END LOOP;
END;
/

select md.WSENSOR_ID, md."DEFINITION", td.month from measurement_dim md , time_dim td, FACT_TABLE fd
where  md.WSENSOR_ID=fd.WSENSOR_ID and td.time_id=fd.time_id ;


select md."DEFINITION", td.WEEK_OF_YEAR,count(fd.result) as NB_measurement_Sensor_Week from measurement_dim md , time_dim td, FACT_TABLE fd
where  md.WSENSOR_ID=fd.WSENSOR_ID and td.time_id=fd.time_id 
group by(md."DEFINITION",td.WEEK_OF_YEAR);

select ld.location, td.month,count(fd.result) as NB_measurement_Location_Month from location_dim ld , time_dim td, FACT_TABLE fd
where  ld.location_id=fd.location_id and td.time_id=fd.time_id 
group by(ld.location,td.month);


select md."DEFINITION", td.year,avg(fd.result) as AVG_measurement_PH_Year from measurement_dim md , time_dim td, FACT_TABLE fd
where  md.WSENSOR_ID=fd.WSENSOR_ID and td.time_id=fd.time_id and md.WSENSOR_ID=61
group by(md."DEFINITION",td.year);


select md."DEFINITION", ld.location, td.year,avg(fd.result) as AVG_measurement_Nitrate_Location_Year from measurement_dim md,location_dim ld , time_dim td, FACT_TABLE fd
where md.WSENSOR_ID=fd.WSENSOR_ID and ld.location_id=fd.location_id and td.time_id=fd.time_id  and md.WSENSOR_ID=117
group by(md."DEFINITION", ld.location,td.year);
