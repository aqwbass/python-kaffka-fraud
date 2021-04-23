CREATE TABLE IF NOT EXISTS public.generates (
    date_time timestamp,
    card_no text,
    lat numeric,
    lon numeric,
    amt_1 bigint,
    fr_acct text,
    to_acct text,
    atm_id text,
    trans text,
    resp text,
    fraud text
);


ALTER TABLE public.generates OWNER TO bass;

--
-- TOC entry 204 (class 1259 OID 16698)
-- Name: pre_items; Type: TABLE; Schema: public; Owner: bass
--

CREATE TABLE IF NOT EXISTS public.pre_items (
    amt_1 text,
    card_no text,
    date_time timestamp,
    fraud text,
    freq_5_minitues integer,
    sumary_5_minitues bigint,
    average_5_minitues numeric,
    diff_5_minitues integer,
    freq_1_hour integer,
    sumary_1_hour bigint,
    average_1_hour numeric,
    diff_1_hour integer,
    freq_2_hour integer,
    sumary_2_hour bigint,
    average_2_hour numeric,
    diff_2_hour integer,
    freq_daily integer,
    sumary_daily bigint,
    average_daily numeric,
    diff_daily integer,
    freq_total integer,
    sumary_total bigint,
    average_total numeric,
    diff_total integer,
    diff_lat_lon numeric,
    is_night text
);


ALTER TABLE public.pre_items OWNER TO bass;

--
-- TOC entry 202 (class 1259 OID 16564)
-- Name: trans_items; Type: TABLE; Schema: public; Owner: bass
--


