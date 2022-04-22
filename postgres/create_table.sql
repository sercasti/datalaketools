CREATE TABLE land_registry_price_paid_uk ( 
                 transaction_uid uuid,      -- Transaction unique identifier	A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.
                 price numeric, -- Sale price stated on the transfer deed.
                 date_of_transfer date, -- Date when the sale was completed, as stated on the transfer deed. 
                 postcode text, -- 	This is the postcode used at the time of the original transaction
                 property_type char(1), -- D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other 
                 new_build_flag boolean, -- Y = a newly built property, N = an established residential building
                 tenure_type char(1),-- F = Freehold, L= Leasehold 
                 PAON text, -- Primary Addressable Object Name
                 SAON text, -- Secondary Addressable Object Name
                 street text,
                 locality text,
                 town_city text,
                 district text,
                 county text,
                 ppd_category_type char(1), -- Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid 
                 record_status char(1) -- A = Addition, C = Change, D = Delete         
               );
--source: https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd