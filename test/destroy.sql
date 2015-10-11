IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customer]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customer]
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[seller]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[seller]
END
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[testSP2]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP2]
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[testSP1]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP1]
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[testSP3]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP3]
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customerkind]
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[sellerkind]
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[selleractivity]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[selleractivity]
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sell]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[sell]
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellsupplement]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].sellsupplement
END
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].customerview
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellerview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].sellerview
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].sellview
GO
