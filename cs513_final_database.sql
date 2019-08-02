CREATE EXTERNAL TABLE cs513.nypl_dish (
  id string, 
  name string, 
  description string, 
  menus_appeared string, 
  times_appeared string, 
  first_appeared string, 
  last_appeared string, 
  lowest_price string, 
  highest_price string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cs513/dish/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
CREATE EXTERNAL TABLE cs513.nypl_menu (
  id string, 
  name string, 
  sponsor string, 
  event string, 
  venue string, 
  place string, 
  physical_description string, 
  occasion string, 
  notes string, 
  call_number string, 
  keywords string, 
  language string, 
  date string, 
  location string, 
  location_type string, 
  currency string, 
  currency_symbol string, 
  status string, 
  page_count string, 
  dish_count string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cs513/menu/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
CREATE EXTERNAL TABLE cs513.nypl_menuitem (
  id string, 
  menu_page_id string, 
  price string, 
  high_price string, 
  dish_id string, 
  created_at string, 
  updated_at string, 
  xpos string, 
  ypos string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cs513/menuitem/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
CREATE EXTERNAL TABLE cs513.nypl_menupage (
  id string, 
  menu_id string, 
  page_number string, 
  image_id string, 
  full_height string, 
  full_width string, 
  uuid string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cs513/menupage/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
create table dish_restaurant_raw
with
(
       format = 'TEXTFILE'
      ,field_delimiter = ','
      ,external_location = 's3://cs513/dish_restaurant_raw/'
)
as
select
      coalesce(concat('"',nypl_menuitem.dish_id,'"'),'""') as dish_id
     ,coalesce(concat('"',replace(replace(replace(replace(trim(nypl_dish.name), chr(34), ''), chr(13), ''), chr(10), ''), chr(9), ''),'"'),'""') as name
     ,coalesce(concat('"',nypl_menupage.menu_id,'"'), '""') as menu_id
     ,coalesce(concat('"',replace(replace(replace(replace(trim(nypl_menu.sponsor), chr(34), ''), chr(13), ''), chr(10), ''), chr(9), ''),'"'), '""') as sponsor
     ,coalesce(concat('"',replace(replace(replace(replace(trim(nypl_menu.place), chr(34), ''), chr(13), ''), chr(10), ''), chr(9), ''),'"'), '""') as place
from nypl_dish
left join nypl_menuitem on nypl_menuitem.dish_id = nypl_dish.id
left join nypl_menupage on nypl_menupage.id = nypl_menuitem.menu_page_id
left join nypl_menu on nypl_menu.id = nypl_menupage.menu_id
where coalesce(concat('"',nypl_menuitem.dish_id,'"'),'""') > '""'
and coalesce(concat('"',replace(replace(replace(replace(trim(nypl_dish.name), chr(34), ''), chr(13), ''), chr(10), ''), chr(9), ''),'"'),'""') > '""'
group by 1,2,3,4,5
order by 2
;
create table unique_restaurant
with
(
       format = 'TEXTFILE'
      ,field_delimiter = ','
      ,external_location = 's3://cs513/unique_restaurant/'
)
as
select distinct
       concat('"',replace(nypl_menu.sponsor,chr(34),chr(39)),'"') as original_sponsor
      ,concat('"',replace(nypl_menu.sponsor,chr(34),chr(39)),'"') as clustered_sponsor
from nypl_menu
;
CREATE EXTERNAL TABLE cs513.openrefine_restaurant (
  original_sponsor string, 
  clustered_sponsor string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://cs513/openrefine_restaurant/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
create table xref_restaurant
with
(
       format = 'TEXTFILE'
      ,field_delimiter = ','
      ,external_location = 's3://cs513/xref_restaurant/'
)
as
select distinct id as menu_id, restaurant_name, restaurant_id
from cs513.nypl_menu
join cs513.openrefine_restaurant
     on original_sponsor = sponsor
join (select distinct RANK() OVER (ORDER BY clustered_sponsor ASC) as restaurant_id, clustered_sponsor as restaurant_name from cs513.openrefine_restaurant) as restaurant
     on restaurant.restaurant_name = clustered_sponsor
order by 3,2,1
;
