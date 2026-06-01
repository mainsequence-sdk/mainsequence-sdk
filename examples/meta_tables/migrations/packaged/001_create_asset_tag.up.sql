CREATE TABLE IF NOT EXISTS public.asset_tag (
    uid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_uid uuid NOT NULL,
    tag varchar(64) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now()
);
