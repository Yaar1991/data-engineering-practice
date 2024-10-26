CREATE TABLE public.products(
    product_id INT NOT NULL,
    product_code INT NOT NULL UNIQUE,
    product_description VARCHAR(25) NOT NULL UNIQUE,
    CONSTRAINT pk_products PRIMARY KEY (product_id)
)
;