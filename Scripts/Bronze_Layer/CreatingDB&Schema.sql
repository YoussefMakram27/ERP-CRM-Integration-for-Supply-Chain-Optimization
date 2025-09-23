/********************************************************************************************
 Script Name   : CreatingDB-BronzeSchema.sql
 Description   : This script creates the db: ERP_CRM, and then creating the schemas: Staging, Bronze, Silver,
				  Gold.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-22
 Modified Date : 2025-09-23  |  Initial 1  |  Added Staging, Silver, Gold Schemas

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : <Special instructions, assumptions, or warnings>

*********************************************************************************************/


if exists (select name from sys.databases where name = 'ERP_CRM')
	print 'DataBase: ERP_CRM Exists'
else
	begin
		use master
		create database ERP_CRM
	end

use ERP_CRM;

create schema Staging;
create schema Bronze;
create schema Silver;
create schema Gold;