-- +goose Up
-- +goose StatementBegin
-- Rename amount column to quantity in exorder table
-- Note: SQLite versions prior to 3.25.0 do not support RENAME COLUMN. 
-- If you are using an older version, you would need to recreate the table.
-- Assuming modern environment.
ALTER TABLE exorder RENAME COLUMN amount TO quantity;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE exorder RENAME COLUMN quantity TO amount;
-- +goose StatementEnd
