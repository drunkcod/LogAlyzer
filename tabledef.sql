
create table [www.example.com](
	[sequence-number] int,
	[date] date,
	[time] time,
	[cs-method] varchar(max),
	[sc-status] int,
	[time-taken] int,
	[s-computername] varchar(max),
	[s-ip] varchar(15),
	[c-ip] varchar(15),
	[cs(User-Agent)] varchar(max),
	[cs-uri-stem] varchar(max),
	[cs-uri-query] varchar(max),
)
