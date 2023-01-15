create table gmo.my_executions ( 
  execution_id  INTEGER  NOT NULL,
  order_id      INTEGER  NOT NULL,
  symbol        STRING   NOT NULL,
  side          STRING   NOT NULL,
  settle_type   STRING   NOT NULL,
  size          STRING   NOT NULL,
  price         NUMERIC  NOT NULL,
  loss_gain     NUMERIC  NOT NULL,
  fee           NUMERIC  NOT NULL,
  timestamp     DATETIME NOT NULL
)
