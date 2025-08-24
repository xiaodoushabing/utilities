"""
DistributedCoordinator - Handles distributed system coordination for LogManager features.

This module provides coordination logic for distributed environments like Ray, Spark, 
or Kubernetes where different nodes may need different behavior.
"""

import os
from typing import Dict, Any


class DistributedCoordinator:
    """
    Handles distributed system coordination for LogManager features.
    
    This class encapsulates the logic for determining which features should be
    enabled based on environment variables and distributed system context.
    
    Perfect for distributed systems where you want:
    - Coordinator/head nodes: Full functionality enabled
    - Worker nodes: Selective feature disabling (e.g., copy disabled)
    """
    
    def __init__(self):
        """Initialize the distributed coordinator."""
        self.copy_enabled = self._check_copy_coordination()
    
    def _check_copy_coordination(self) -> bool:
        """
        Simple check if copy should be enabled using environment variables.
        
        Environment Variables:
            DISABLE_COPY: Set to 'true' to disable copy
        
        Returns:
            bool: True if copy should be enabled, False otherwise.
        """
        enabled = os.getenv('DISABLE_COPY', '').lower() != 'true'
        if not enabled:
            print("Copy disabled via DISABLE_COPY environment variable")
        else:
            print("Copy enabled (default behavior)")
        return enabled

    def get_copy_status(self) -> Dict[str, Any]:
        """
        Get current copy status and environment information.
        
        Returns:
            dict: Status information including enabled state and reason.
        """
        disable_copy = os.getenv('DISABLE_COPY', '')
        
        return {
            'copy_enabled': self.copy_enabled,
            'reason': 'DISABLE_COPY=true' if disable_copy.lower() == 'true' else 'Default behavior (enabled)',
            'environment_variable': {'DISABLE_COPY': disable_copy}
        }
