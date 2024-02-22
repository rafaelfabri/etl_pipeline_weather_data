from typing import Union, Dict

GenericSchema = Dict[str, Union[str, float, int]]

ContratoSchema: GenericSchema = {
    "validdate"         : str,
    "t_2m:C"            : float,
    "precip_1h:mm"      : float,
    "wind_speed_10m:ms" : float
    }