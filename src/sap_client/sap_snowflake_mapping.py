from keboola.component.dao import SupportedDataTypes

SAP_TO_SNOWFLAKE_MAP = {
    'CHAR': SupportedDataTypes.STRING.value,
    'NUM': SupportedDataTypes.NUMERIC.value,
    'STRING': SupportedDataTypes.STRING.value,
    'BOOLEAN': SupportedDataTypes.BOOLEAN.value,
    'INT': SupportedDataTypes.INTEGER.value,
    'INT8': SupportedDataTypes.INTEGER.value,
    'PACKED': SupportedDataTypes.NUMERIC.value,  # Guess, please verify
    'DECFLOAT16': SupportedDataTypes.FLOAT.value,  # Guess, please verify
    'DECFLOAT34': SupportedDataTypes.FLOAT.value,  # Guess, please verify
    'FLOAT': SupportedDataTypes.FLOAT.value,
    'DATE': SupportedDataTypes.DATE.value,
    'TIME': SupportedDataTypes.TIMESTAMP.value,  # Guess, assuming TIME is represented as TIMESTAMP
    'UTCLONG': SupportedDataTypes.TIMESTAMP.value,  # Guess, please verify
    'HEX': SupportedDataTypes.STRING.value,  # Assuming hexadecimal values are stored as strings
    'XSTRING': SupportedDataTypes.STRING.value  # Guess, please verify
}
