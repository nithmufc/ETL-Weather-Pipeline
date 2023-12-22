import unittest
from etl_script import transform  

class TestTransformFunction(unittest.TestCase):

    def test_temperature_conversion(self):
        # Test when temperature in Kelvin is 273.15
        data = {"main": {"temp": 273.15}}
        result = transform(data, None)
        self.assertEqual(result["temperature_celsius"], 0.0)

        # Test when temperature is None (handling missing value)
        data = {"main": {"temp": None}}
        result = transform(data, None)
        self.assertEqual(result["temperature_celsius"], 0.0)

        

if __name__ == '__main__':
    unittest.main()
