from prophet import Prophet
from bin.config import *


class Model:
    """
    Represents a forecasting model using the Prophet library.

    Attributes:
        model: Prophet object representing the forecasting model.
        history_prediction_df: Property representing the history prediction DataFrame.
        future_prediction_df: Property representing the future prediction DataFrame.
        range_history_timestamp: Property representing the range of history timestamps.

    Methods:
        fit(history_df: DataFrame): Fits the forecasting model to the given historical data.
        forecasting(periods: int = 4, freq: str = "15Min"): Perform forecasting for future periods
            based on the trained model.
    """

    def __init__(self, limit_range_datetime: Optional[timedelta]) -> None:
        self.model: Prophet = Prophet(interval_width=0.95)
        self.limit_range_datetime: Optional[timedelta] = limit_range_datetime
        self.__min_history_ds: Optional[datetime] = None
        self.__max_history_ds: Optional[datetime] = None
        self.__all_prediction_df: Optional[pd.DataFrame] = None
        self.__history_prediction_df: Optional[pd.DataFrame] = None
        self.__future_prediction_df: Optional[pd.DataFrame] = None

    def fit(self, history_df: pd.DataFrame) -> None:
        """
        Fits the forecasting model to the given historical data.

        Args:
            history_df (pd.DataFrame): The historical data DataFrame containing 'ds' and 'y'
                columns.

        Raises:
            ValueError: If the input DataFrame does not contain 'ds' and 'y' columns.

        Returns:
            None
        """
        if "ds" not in history_df.columns or "y" not in history_df.columns:
            raise ValueError("The input DataFrame must contain 'ds' and 'y' columns.")

        self.__max_history_ds = history_df["ds"].max()
        if self.limit_range_datetime:
            history_df = history_df[
                history_df["ds"] > self.__max_history_ds - self.limit_range_datetime
            ]

        self.__min_history_ds = history_df["ds"].min()
        self.model.fit(history_df)

    def forecasting(self, periods: int = 4, freq: str = "15Min") -> pd.DataFrame:
        """
        Perform forecasting for future periods based on the trained model.

        Args:
            periods (int): Number of future periods to forecast. Default is 4.
            freq (str): Frequency of the forecasted periods. Default is "15Min".

        Returns:
            pd.DataFrame: DataFrame containing the forecasted values for the future periods.
        """
        self.__all_prediction_df = self.model.make_future_dataframe(
            periods=periods,
            freq=freq,
            include_history=True,
        )

        self.__all_prediction_df = self.model.predict(self.__all_prediction_df)
        self.__all_prediction_df = self.__all_prediction_df[["ds", "yhat"]]
        self.__history_prediction_df = self.__all_prediction_df[
            self.__all_prediction_df["ds"] <= self.__max_history_ds
        ]
        self.__future_prediction_df = self.__all_prediction_df[
            self.__all_prediction_df["ds"] > self.__max_history_ds
        ]

        return self.__future_prediction_df

    @property
    def history_prediction_df(self) -> pd.DataFrame:
        """
        Returns the history prediction DataFrame.

        Raises:
            ValueError: If the history prediction DataFrame is not initialized.
                Please call forecasting() first.

        Returns:
            pd.DataFrame: The history prediction DataFrame.
        """
        if self.__history_prediction_df is None:
            raise ValueError(
                "The history prediction DataFrame is not initialized. Please call forecasting() first."
            )
        return self.__history_prediction_df

    @property
    def future_prediction_df(self) -> pd.DataFrame:
        """
        Returns the future prediction DataFrame.

        Raises:
            ValueError: If the future prediction DataFrame is not initialized.

        Returns:
            pd.DataFrame: The future prediction DataFrame.
        """
        if self.__future_prediction_df is None:
            raise ValueError(
                "The future prediction DataFrame is not initialized. Please call forecasting() first.",
            )
        return self.__future_prediction_df

    @property
    def range_history(self) -> tuple[datetime, datetime]:
        """
        Returns the range of history timestamps.
        
        Raises:
            ValueError: If the future prediction DataFrame is not initialized.

        Returns:
            tuple[datetime, datetime]: A tuple containing the minimum and maximum history timestamps.
        """
        if self.__min_history_ds is None or self.__max_history_ds is None:
            raise ValueError(
                "The history DataFrame is not initialized. Please call fit() first."
            )
        return self.__min_history_ds - timedelta(minutes=15), self.__max_history_ds
