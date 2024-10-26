CREATE TABLE public.transactions(
    transaction_id VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    product_id INT NOT NULL,
    product_code INT NOT NULL,
    product_description VARCHAR(25) NOT NULL,
    quantity INT NOT NULL,
    account_id INT NOT NULL,
    CONSTRAINT pk_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES accounts(customer_id),
    CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES products(product_code),
    CONSTRAINT fk_product_description FOREIGN KEY (product_description) REFERENCES products(product_description)
    )
;

CREATE INDEX idx_transactions_date ON public.transactions (transaction_date)
;
CREATE INDEX idx_transactions_product_id ON public.transactions (product_id);

CREATE INDEX idx_transactions_account_id ON public.transactions (account_id)
;