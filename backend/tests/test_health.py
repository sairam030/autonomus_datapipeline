import unittest

from fastapi.testclient import TestClient

from backend.app.main import app


class HealthEndpointsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "healthy"})

    def test_root_endpoint(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get("service"), "Autonomous Pipeline API")
        self.assertEqual(payload.get("status"), "running")


if __name__ == "__main__":
    unittest.main()
