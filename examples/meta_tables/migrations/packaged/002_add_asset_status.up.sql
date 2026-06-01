ALTER TABLE public.asset
    ADD COLUMN IF NOT EXISTS status varchar(32) NOT NULL DEFAULT 'active';
