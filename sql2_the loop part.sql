  INSERT INTO P_hang_T.table2
  SELECT 
  HOT.A,
  HOT.B,
  HOT.C,
  HOT.AUCT_END_DT,
  HOT.AUCT_START_DT,
  
  FROM table1 AS HOT

  WHERE     1 = 1
  AND       HOT.AUCT_END_DT >= @START_DT
  AND       HOT.AUCT_START_DT BETWEEN @START_DT AND @END_DT

