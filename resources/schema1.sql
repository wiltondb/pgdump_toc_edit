create database test3
use test3
go

-- namespace
create schema schema1
go

-- domain

create type domain1 from nvarchar(max) not null
create type schema1.domain2 from nvarchar(max) not null
go

-- table type

create type tabletype1 as table (
	id int,
	val domain1
)
go

create type schema1.tabletype2 as table (
	id int,
	val schema1.domain2
)
go

-- function

create function func1(@param1 int) returns int as
begin
	return @param1 + 1
end
go

create function schema1.func2(@param1 int) returns int as
begin
	return @param1 + 1
end
go

create function func3(@param1 domain1) returns domain1 as
begin
	return concat('Hello ', @param1);
end
go

create function schema1.func4(@param1 domain1) returns domain1 as
begin
	return concat('Hello ', @param1);
end
go

create function func5(@param1 domain1) returns table
as return (
	select cast(concat('Hello ', @param1) as domain1)
)
go

create function schema1.func6(@param1 domain1) returns table
as return (
	select cast(concat('Hello ', @param1) as schema1.domain2)
)
go

create function func7(@param1 domain1)
returns @ret1 table (
	id1 int,
	val1 domain1
) as
begin
	insert @ret1
	select 42, @param1
	return;
end
go

create function schema1.func8(@param1 domain1)
returns @ret1 table (
	id1 int,
	val1 domain1
) as
begin
	insert @ret1
	select 42, @param1
	return;
end
go

-- procedure

create procedure proc1(@param1 int, @param2 int out) as
begin
	set @param2 = @param1 + 1;
end
go

create procedure schema1.proc2(@param1 int, @param2 int out) as
begin
	set @param2 = @param1 + 1;
end
go

create procedure proc3(@param1 domain1, @param2 domain1 out) as
begin
	set @param2 = concat('Hello ', @param1);
end
go

create procedure schema1.proc4(@param1 schema1.domain2, @param2 schema1.domain2 out) as
begin
	set @param2 = concat('Hello ', @param1);
end
go

create procedure proc5(@param1 tabletype1 readonly) as
begin
	select * from @param1;
end
go

create procedure schema1.proc6(@param1 schema1.tabletype2 readonly) as
begin
	select * from @param1;
end
go

-- table

create table tab1 (
	id int primary key,
	val domain1
);
go

create table schema1.tab2 (
	id int primary key,
	val schema1.domain2
);
go

drop table schema1.tab3

create table schema1.tab3 (
	id int primary key,
	val schema1.domain2,
	parent_id int references schema1.tab2(id)
);
go

-- index

create clustered index index1 ON tab1 (id);
go

create clustered index index2 ON schema1.tab2 (id);
go

-- trigger

create trigger trig1
on tab1
after insert, update
as
insert into schema1.tab2(id, val)
select id, val from inserted
go

create trigger schema1.trig2
on schema1.tab2
after insert, update
as
insert into schema1.tab3(id, val, parent_id)
select id + 1, val, id from inserted
go

-- constraint

alter table tab1
add constraint constr1 check (id >= 42);

alter table schema1.tab2
add constraint constr2 check (id >= 42); 
