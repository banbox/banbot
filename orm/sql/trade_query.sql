
-- name: GetTask :one
select * from bottask
where id = ?;

-- name: FindTask :one
select * from bottask
where mode = ? and name = ?
order by create_at desc
    limit 1;

-- name: AddTask :one
insert into bottask
("mode", "name", "create_at", "start_at", "stop_at", "info")
values (?, ?, ?, ?, ?, ?)
    returning *;

-- name: ListTasks :many
select * from bottask
order by id;



-- name: ListTaskPairs :many
select symbol from iorder
where task_id = ?
  and enter_at >= ?
  and enter_at <= ?;



-- name: GetIOrder :one
select * from iorder
where id = ?;

-- name: GetExOrders :many
select * from exorder
where inout_id=?;

-- name: GetTaskPairs :many
select distinct symbol from iorder
where task_id=? and enter_at>=? and enter_at<=?;

-- name: AddIOrder :one
insert into iorder ("task_id", "symbol", "sid", "timeframe", "short", "status",
                    "enter_tag", "init_price", "stop", "quote_cost", "exit_tag", "leverage",
                    "enter_at", "exit_at", "strategy", "stg_ver", "max_pft_rate", "max_draw_down",
                    "profit_rate", "profit", "info")
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    RETURNING id;

-- name: SetIOrder :exec
update iorder set
                  "task_id" = ?,
                  "symbol" = ?,
                  "sid" = ?,
                  "timeframe" = ?,
                  "short" = ?,
                  "status" = ?,
                  "enter_tag" = ?,
                  "stop" = ?,
                  "init_price" = ?,
                  "quote_cost" = ?,
                  "exit_tag" = ?,
                  "leverage" = ?,
                  "enter_at" = ?,
                  "exit_at" = ?,
                  "strategy" = ?,
                  "stg_ver" = ?,
                  "max_pft_rate" = ?,
                  "max_draw_down" = ?,
                  "profit_rate" = ?,
                  "profit" = ?,
                  "info" = ?
WHERE id = ?;

-- name: AddExOrder :one
insert into exorder ("task_id", "inout_id", "symbol", "enter", "order_type", "order_id", "side",
                     "create_at", "price", "average", "quantity", "filled", "status", "fee", "fee_quote", "fee_type", "update_at")
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    RETURNING id;

-- name: SetExOrder :exec
update exorder set
                   "task_id" = ?,
                   "inout_id" = ?,
                   "symbol" = ?,
                   "enter" = ?,
                   "order_type" = ?,
                   "order_id" = ?,
                   "side" = ?,
                   "create_at" = ?,
                   "price" = ?,
                   "average" = ?,
                   "quantity" = ?,
                   "filled" = ?,
                   "status" = ?,
                   "fee" = ?,
                   "fee_quote" = ?,
                   "fee_type" = ?,
                   "update_at" = ?
where id = ?;

