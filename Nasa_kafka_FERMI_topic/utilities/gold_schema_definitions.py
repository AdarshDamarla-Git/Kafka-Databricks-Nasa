dim_event_timing_schema="""
    timing_sk INT NOT NULL,
    grb_date_raw STRING,
    grb_time_raw STRING,
    notice_date_raw STRING,

    CONSTRAINT pk_dim_event_timing
        PRIMARY KEY (timing_sk) RELY
"""


dim_spatial_coords_schema="""
    spatial_sk INT,
    sun_moon_sk INT NOT NULL,
    right_ascension STRING,
    declination STRING,
    galactic_coords STRING,
    ecliptic_coords STRING,

    CONSTRAINT pk_dim_spatial_coords
        PRIMARY KEY (spatial_sk) RELY,

    CONSTRAINT fk_spatial_to_sun_moon
        FOREIGN KEY (sun_moon_sk)
        REFERENCES kafka_nasa_fermi_topic.gold.dim_sun_moon_coords(sun_moon_sk)
        RELY
"""


dim_sun_moon_coords_schema="""
    sun_moon_sk INT NOT NULL,
    sun_position STRING,
    moon_position STRING,
    moon_illumination STRING,

    CONSTRAINT pk_dim_sun_moon_coords
        PRIMARY KEY (sun_moon_sk) RELY
"""


fact_gcn_notices_schema="""
    trigger_num BIGINT,
    record_num INT,

    kafka_key STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,

    timing_sk INT,
    spatial_sk INT,

    notice_title STRING,
    notice_type STRING,
    loc_algorithm STRING,
    comments STRING,
    lightcurve_url STRING,
    location_url STRING,

    grb_error_deg DOUBLE,
    grb_phi_deg DOUBLE,
    grb_theta_deg DOUBLE,
    sun_dist_deg DOUBLE,
    moon_dist_deg DOUBLE,

    CONSTRAINT fk_fact_to_timing
        FOREIGN KEY (timing_sk)
        REFERENCES kafka_nasa_fermi_topic.gold.dim_event_timing(timing_sk)
        RELY,

    CONSTRAINT fk_fact_to_spatial
        FOREIGN KEY (spatial_sk)
        REFERENCES kafka_nasa_fermi_topic.gold.dim_spatial_coords(spatial_sk)
        RELY
"""