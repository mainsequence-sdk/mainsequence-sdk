import inspect
import json
import traceback

from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.utils import is_jupyter_environment

from .resource_factory.base_factory import send_resource_to_backend
from .utils import logger


class TDAGAgent:

    def __init__(self):
        self.logger = logger
        self.logger.info("Setup TDAG Agent successfull")

    def query_agent(self, query):
        try:
            from mainsequence.client.models_tdag import query_agent

            payload = {
                "query": query,
            }
            response = query_agent(json_payload=payload)
            if response.status_code not in [200, 201]:
                raise Exception(response.text)

            answer = response.json()["agent_response"]
        except Exception as e:
            self.logger.warning(f"Could not get answer from Agent {e}")
            traceback.print_exc()
            return None

        return answer

    def generate_portfolio(self, cls, signal_description=None):
        full_signal_description = f"Create me a default portfolio using the {cls.__name__} signal."
        if signal_description is not None:
            full_signal_description += f"\n{signal_description}"
        else:
            full_signal_description += "Use NVDA, AAPL and GOOGL for the assets universe."

        if is_jupyter_environment():
            code = cls.get_source_notebook()
        else:
            code = inspect.getsource(cls)
        attributes = {"code": code}
        send_resource_to_backend(cls, attributes=attributes)

        payload = {
            "strategy_name": cls.__name__,
            "signal_description": full_signal_description,
        }
        self.logger.info(f"Get configuration for {cls.__name__} ...")
        payload = json.loads(json.dumps(payload))
        try:
            from mainsequence.client.models_tdag import create_configuration_for_strategy

            response = create_configuration_for_strategy(json_payload=payload)
            if response.status_code not in [200, 201]:
                raise Exception(response.text)

            generated_configuration = response.json()["generated_configuration"]["configuration"][
                "portfolio_configuration"
            ]
            portfolio = PortfolioInterface(generated_configuration)

            self.logger.info(f"Received configuration:\n{portfolio}")

        except Exception as e:
            self.logger.warning(f"Could not get configuration from TSORM {e}")
            traceback.print_exc()
            return None

        return portfolio
