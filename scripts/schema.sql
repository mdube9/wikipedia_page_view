
create table if not exists blacklist_domains
(
	domain varchar(20),
	page text
)
;

create table if not exists main_page_view
(
	date integer,
	hour integer,
	domain varchar(20),
	page text,
	page_view_count integer
)
;

create table if not exists file_track
(
	date integer,
	hour integer,
	file_name varchar(60) PRIMARY KEY
)
;
