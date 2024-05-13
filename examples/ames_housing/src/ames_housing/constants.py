AMES_HOUSING_DATA_SET_URL = "http://jse.amstat.org/v19n3/decock/AmesHousing.txt"
AMES_HOUSING_DATA_SET_SEPARATOR = "\t"

SELECTED_FEATURES = {
    "nominal": ["ms_zoning", "lot_shape", "land_contour"],
    "ordinal": ["land_slope", "overall_qual", "overall_cond"],
    "numerical": ["lot_frontage", "lot_area", "mas_vnr_area"],
}

TARGET = "sale_price"

RANDOM_STATE = 42

MLFLOW_TRACKING_URL = "http://localhost:5000"
MLFLOW_EXPERIMENT = "Ames housing price prediction"

LAKEFS_REPOSITORY = "ames-housing"
LAKEFS_BRANCH = "main"

LAKEFS_DATA_PATH = ["data"]
LAKEFS_MODEL_PATH = ["models"]
