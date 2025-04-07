#!/usr/bin/env python3
from dataclasses import dataclass, asdict
from typing import Dict, Set, Optional, List, Any
from datetime import datetime

@dataclass
class ContactInfo:
    """Data class for faculty contact information"""
    name: str
    office: str  # College
    department: str  # Program
    profile_url: str
    email: str = ""
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ContactInfo':
        """Create from dictionary"""
        return cls(**data)

class ScraperStatistics:
    """Class to track scraping statistics"""
    
    def __init__(self, base_url: str, num_nodes: int, scrape_time_minutes: int):
        self.base_url = base_url
        self.num_nodes = num_nodes
        self.scrape_time_minutes = scrape_time_minutes
        self.start_time = datetime.now().isoformat()
        self.end_time = None
        
        # Statistics counters
        self.colleges_count = 0
        self.programs_visited = 0
        self.total_pages_visited = 0
        self.total_emails_recorded = 0
        
        # Program-specific tracking
        self.programs_with_faculty_url: Dict[str, bool] = {}
        self.programs_without_faculty_url: Dict[str, bool] = {}
        self.program_personnel_count: Dict[str, int] = {}
        self.program_complete_records: Dict[str, int] = {}
        self.program_incomplete_records: Dict[str, int] = {}
        
        # Node tracking for distributed system
        self.node_contributions: Dict[str, int] = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        self.end_time = datetime.now().isoformat()
        return {
            "base_url": self.base_url,
            "num_nodes": self.num_nodes,
            "scrape_time_minutes": self.scrape_time_minutes,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "colleges_count": self.colleges_count,
            "programs_visited": self.programs_visited,
            "total_pages_visited": self.total_pages_visited,
            "total_emails_recorded": self.total_emails_recorded,
            "programs_with_faculty_url": list(self.programs_with_faculty_url.keys()),
            "programs_without_faculty_url": list(self.programs_without_faculty_url.keys()),
            "program_personnel_count": self.program_personnel_count,
            "program_complete_records": self.program_complete_records,
            "program_incomplete_records": self.program_incomplete_records,
            "node_contributions": self.node_contributions
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScraperStatistics':
        """Create from dictionary"""
        stats = cls(
            data["base_url"],
            data["num_nodes"],
            data["scrape_time_minutes"]
        )
        stats.start_time = data["start_time"]
        stats.end_time = data["end_time"]
        stats.colleges_count = data["colleges_count"]
        stats.programs_visited = data["programs_visited"]
        stats.total_pages_visited = data["total_pages_visited"]
        stats.total_emails_recorded = data["total_emails_recorded"]
        
        # Convert lists back to dictionaries
        stats.programs_with_faculty_url = {p: True for p in data["programs_with_faculty_url"]}
        stats.programs_without_faculty_url = {p: True for p in data["programs_without_faculty_url"]}
        
        stats.program_personnel_count = data["program_personnel_count"]
        stats.program_complete_records = data["program_complete_records"]
        stats.program_incomplete_records = data["program_incomplete_records"]
        stats.node_contributions = data["node_contributions"]
        
        return stats

@dataclass
class NodeStatus:
    """Data class for tracking worker node status"""
    node_id: str
    status: str  # 'online', 'offline', 'busy'
    last_heartbeat: str  # ISO format timestamp
    current_task: Optional[str] = None
    tasks_completed: int = 0
    errors_encountered: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeStatus':
        """Create from dictionary"""
        return cls(**data)