INSERT INTO [testdatabase].[dbo].[ADJUSTMENT] VALUES (912, 950000100107, 9933, 102, 506161, 775324, 431703, 28440200.7189,N'iT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89');
INSERT INTO [testdatabase].[dbo].[ADJUSTMENT] VALUES (912, 950000100108, 9933, 102, 506161, 775324, 431703, 28440200.7189,N'iT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89');
update [testdatabase].[dbo].[ADJUSTMENT] set DESCR = N'iTTTz' where adj_id = 950000100107;
update [testdatabase].[dbo].[ADJUSTMENT] set DESCR = N'iTTTz' where adj_id = 950000100108;
delete from [testdatabase].[dbo].[ADJUSTMENT] where adj_id = 950000100107;
update [testdatabase].[dbo].[ADJUSTMENT] set DESCR = N'iTTTzy' where adj_id = 950000100108;