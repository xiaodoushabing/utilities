from typing import Optional

from hydra.logging.promtail import PromtailAgent

class PromtailManager:
    def __init__(self, config: dict = {}):
        self.instance_name = config.get("instance_name", "")
        self.target_paths = config.get("target_paths", [])
        self.log_level = config.get("log_level", "").upper()
        self.static_labels = config.get("static_labels", {})
        self.promtail_agent = None
    
    def start_promtail(self, config: Optional[dict] = None):
        if config:
            self.instance_name = config.get("instance_name", self.instance_name)
            self.target_paths = config.get("target_paths", self.target_paths)
            self.log_level = config.get("log_level", self.log_level)
            self.static_labels = config.get("static_labels", self.static_labels)

        all_kwargs = {
            "instance_name": self.instance_name,
            "target_paths": self.target_paths,
            "log_level": self.log_level,
        }

        kwargs = {k: v for k, v in all_kwargs.items() if v}

        if "instance_name" not in kwargs:
            raise ValueError("Promtail instance name is required")
        
        if "target_paths" not in kwargs:
            raise ValueError("Promtail target paths are required")
        
        self.promtail_agent = PromtailAgent(**kwargs)

        if self.static_labels:
            labels = self.promtail_agent.promtail_config.scrape_configs[0]["pipeline_stages"][0]["static_labels"]
            for k, v in self.static_labels.items():
                if v is not None and v != "":
                    labels[k] = v

        self.promtail_agent.start()
        print("PromtailAgent started")

    def stop_promtail(self):
        try:
            self.promtail_agent.stop()
            self.promtail_agent = None
            print("PromtailAgent stopped")
        except Exception as e:
            print(f"WARNING: Could not stop PromtailAgent: {e}")

    def cleanup(self):
        if self.promtail_agent == None:
            print("No PromtailAgent to clean up.")
            return
        print("PromtailManager cleanup initiated...")
        self.stop_promtail()
        print("PromtailManager cleanup completed.")