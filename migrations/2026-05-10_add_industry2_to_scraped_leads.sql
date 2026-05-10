-- Add industry2: a more email-friendly industry label, deterministically
-- mapped from `industry`. NULL stays NULL.
--
-- Applied via Supabase MCP on 2026-05-10. This file is the audit copy —
-- the migration ran on production through the MCP migration table.
ALTER TABLE raw.scraped_leads ADD COLUMN IF NOT EXISTS industry2 text;

COMMENT ON COLUMN raw.scraped_leads.industry2 IS
  'Email-friendly industry label, deterministically mapped from industry. '
  'See leadgen/industry2.py for the canonical map. NULL when industry IS NULL.';

-- One-shot backfill from the canonical mapping. Rows with industry IS NULL
-- stay NULL.
UPDATE raw.scraped_leads
   SET industry2 = m.industry2
  FROM (VALUES
    ('Business Consulting',          'Consulting firm'),
    ('Clinic Services',              'Medical Clinic'),
    ('Restaurants and Bars',         'Restaurant'),
    ('Med Spa',                      'Med spa'),
    ('Hotels and Leisure',           'Hotel'),
    ('Elder and Disabled Care',      'Senior care home'),
    ('Consumer Services',            'Local service provider'),
    ('Healthcare Software',          'Healthcare software'),
    ('Hospitals',                    'Hospital'),
    ('Retail Stores',                'Retail store'),
    ('Business Software',            'Software'),
    ('Dentist',                      'Dentist'),
    ('Medical Devices',              'Medical device supplier'),
    ('Real Estate Services',         'Real estate agency'),
    ('Food and Beverage Brands',     'F&B brand'),
    ('Media and Publishing',         'Media company'),
    ('Business Services',            'Business service provider'),
    ('Home Goods',                   'Home goods store'),
    ('Education and Training',       'Training center'),
    ('Personal Care Products',       'Personal care products'),
    ('Wholesale Distributors',       'Wholesale distributor'),
    ('Immigration Lawyer',           'Immigration lawyer'),
    ('Industrial Manufacturers',     'Manufacturer'),
    ('Construction and Engineering', 'Contractor'),
    ('Medical Laboratories',         'Medical lab'),
    ('HR and Staffing',              'Staffing agency'),
    ('Pharma and Biotech',           'Biotech company'),
    ('Interior Design',              'Interior designer'),
    ('Electronics and Hardware',     'Electronics store'),
    ('Financial Services',           'Financial advisor'),
    ('Consumer Products',            'Consumer products'),
    ('Managed Care',                 'Managed care provider'),
    ('Agriculture and Farming',      'Farm'),
    ('Logistics and Supply Chain',   'Logistics company'),
    ('Insurance Providers',          'Insurance company'),
    ('Medical Supply Distributors',  'Medical supplier'),
    ('Automotive',                   'Car dealership'),
    ('Packaging Suppliers',          'Packaging supplier'),
    ('Real Estate and Trust Lawyer', 'Estate lawyer'),
    ('Apparel and Footwear',         'Clothing brand'),
    ('Holding Companies',            'Holding company'),
    ('Legal Services',               'Lawyer'),
    ('Family Lawyer',                'Family lawyer'),
    ('Transportation Services',      'Transportation company'),
    ('Environmental Services',       'Environmental company'),
    ('Chemicals and Materials',      'Chemical supplier'),
    ('Personal Injury Lawyer',       'Personal injury lawyer'),
    ('Security Services',            'Security Services'),
    ('Aerospace and Defense',        'Aerospace company'),
    ('Energy and Utilities',         'Energy provider'),
    ('Government Services',          'Administrative services company'),
    ('Accounting and Tax Services',  'Accounting services'),
    ('Telecommunications',           'Telecom company')
  ) AS m(industry, industry2)
 WHERE raw.scraped_leads.industry = m.industry
   AND raw.scraped_leads.industry2 IS DISTINCT FROM m.industry2;
