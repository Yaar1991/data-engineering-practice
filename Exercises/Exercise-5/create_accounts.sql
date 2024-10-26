
CREATE TABLE public.accounts
(
    customer_id INT         NOT NULL,
    first_name  VARCHAR(25) NOT NULL,
    last_name   VARCHAR(25) NOT NULL,
    address_1   VARCHAR(25) NOT NULL,
    address_2   VARCHAR(25),
    city        VARCHAR(25),
    state       VARCHAR(10),
    zip_code    INT,
    join_date   DATE,
    CONSTRAINT pk_accounts PRIMARY KEY (customer_id)
)
;

CREATE INDEX idx_accounts_join_date ON public.accounts (join_date)
;