"""
GLYPH Streaming Validator for Python

Validates GLYPH tool calls incrementally as tokens arrive from an LLM.

This enables:
  - Early tool detection: Know the tool name before full response
  - Early rejection: Stop on unknown tools mid-stream  
  - Incremental validation: Check constraints as tokens arrive
  - Latency savings: Reject bad payloads without waiting for completion

Example:
    >>> registry = ToolRegistry()
    >>> registry.add_tool("search", {
    ...     "query": {"type": "str", "required": True},
    ...     "max_results": {"type": "int", "min": 1, "max": 100, "default": 10}
    ... })
    >>>
    >>> validator = StreamingValidator(registry)
    >>>
    >>> for token in llm_stream:
    ...     result = validator.push_token(token)
    ...     if result.should_cancel:
    ...         await llm.cancel()  # Stop bleeding tokens!
    ...         break
    ...
    ...     if result.complete and result.valid:
    ...         return result.parsed()
"""

import re
import time
from typing import Dict, List, Optional, Any, Pattern
from dataclasses import dataclass, field as dataclass_field
from enum import Enum


class ArgType(str, Enum):
    """Argument type enumeration."""
    STRING = "str"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    LIST = "list"


@dataclass
class ArgSchema:
    """Schema for a tool argument."""
    name: str
    type: ArgType
    required: bool = False
    min: Optional[float] = None
    max: Optional[float] = None
    min_len: Optional[int] = None
    max_len: Optional[int] = None
    pattern: Optional[str] = None
    enum: Optional[List[str]] = None
    default: Optional[Any] = None
    
    _pattern_re: Optional[Pattern] = None
    
    def __post_init__(self):
        """Compile regex pattern if provided."""
        if self.pattern:
            self._pattern_re = re.compile(self.pattern)
    
    def validate(self, value: Any) -> Optional[str]:
        """
        Validate a value against this schema.
        Returns error message if invalid, None if valid.
        """
        if value is None:
            if self.required:
                return f"Missing required argument: {self.name}"
            return None
        
        # Type checking
        if self.type == ArgType.STRING:
            if not isinstance(value, str):
                return f"Argument {self.name} must be string, got {type(value).__name__}"
            
            if self.min_len is not None and len(value) < self.min_len:
                return f"Argument {self.name} too short (min {self.min_len})"
            
            if self.max_len is not None and len(value) > self.max_len:
                return f"Argument {self.name} too long (max {self.max_len})"
            
            if self._pattern_re and not self._pattern_re.match(value):
                return f"Argument {self.name} does not match pattern"
            
            if self.enum and value not in self.enum:
                return f"Argument {self.name} not in enum: {self.enum}"
        
        elif self.type == ArgType.INT:
            if not isinstance(value, int) or isinstance(value, bool):
                return f"Argument {self.name} must be int"
            
            if self.min is not None and value < self.min:
                return f"Argument {self.name} < min {self.min}"
            
            if self.max is not None and value > self.max:
                return f"Argument {self.name} > max {self.max}"
        
        elif self.type == ArgType.FLOAT:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return f"Argument {self.name} must be float"
            
            if self.min is not None and value < self.min:
                return f"Argument {self.name} < min {self.min}"
            
            if self.max is not None and value > self.max:
                return f"Argument {self.name} > max {self.max}"
        
        elif self.type == ArgType.BOOL:
            if not isinstance(value, bool):
                return f"Argument {self.name} must be bool"
        
        elif self.type == ArgType.LIST:
            if not isinstance(value, list):
                return f"Argument {self.name} must be list"
        
        return None


@dataclass
class ToolSchema:
    """Schema for a tool and its arguments."""
    name: str
    args: Dict[str, ArgSchema] = dataclass_field(default_factory=dict)
    description: str = ""
    
    def validate(self, fields: Dict[str, Any]) -> Optional[str]:
        """
        Validate all fields against this tool schema.
        Returns error message if invalid, None if valid.
        """
        # Check for unknown fields
        for key in fields:
            if key not in self.args:
                return f"Unknown field: {key}"
        
        # Check all required args are present
        for arg_name, arg_schema in self.args.items():
            if arg_schema.required and arg_name not in fields:
                return f"Missing required argument: {arg_name}"
        
        # Validate each field
        for arg_name, value in fields.items():
            arg_schema = self.args[arg_name]
            error = arg_schema.validate(value)
            if error:
                return error
        
        return None


class ToolRegistry:
    """Registry of allowed tools for validation."""
    
    def __init__(self):
        self.tools: Dict[str, ToolSchema] = {}
    
    def add_tool(self, name: str, args: Dict[str, Dict[str, Any]], description: str = ""):
        """
        Add a tool to the registry.
        
        Args:
            name: Tool name
            args: Dict of arg_name -> arg_config
            description: Human-readable description
            
        Example:
            registry.add_tool("search", {
                "query": {"type": "str", "required": True},
                "max_results": {"type": "int", "min": 1, "max": 100, "default": 10}
            })
        """
        arg_schemas = {}
        for arg_name, arg_config in args.items():
            arg_type = ArgType(arg_config.get("type", "str"))
            arg_schemas[arg_name] = ArgSchema(
                name=arg_name,
                type=arg_type,
                required=arg_config.get("required", False),
                min=arg_config.get("min"),
                max=arg_config.get("max"),
                min_len=arg_config.get("min_len"),
                max_len=arg_config.get("max_len"),
                pattern=arg_config.get("pattern"),
                enum=arg_config.get("enum"),
                default=arg_config.get("default"),
            )
        
        self.tools[name] = ToolSchema(name=name, args=arg_schemas, description=description)
    
    def is_allowed(self, name: str) -> bool:
        """Check if a tool is registered."""
        return name in self.tools
    
    def get_tool(self, name: str) -> Optional[ToolSchema]:
        """Get a tool schema."""
        return self.tools.get(name)


class ValidatorState(Enum):
    """State machine states for streaming validation."""
    WAITING = "waiting"          # Waiting for opening brace
    IN_OBJECT = "in_object"      # Inside the main object
    COMPLETE = "complete"        # Object fully parsed
    ERROR = "error"              # Unrecoverable error


@dataclass
class TimelineEvent:
    """A significant event during validation."""
    event: str              # TOOL_DETECTED, ERROR, COMPLETE
    token_count: int        # Token number
    char_count: int         # Character position
    elapsed: float          # Seconds since start
    detail: str = ""        # Additional info


@dataclass
class StreamValidationResult:
    """Result of a single push_token call."""
    tool_name: Optional[str] = None
    fields: Dict[str, Any] = dataclass_field(default_factory=dict)
    errors: List[str] = dataclass_field(default_factory=list)
    
    # State
    complete: bool = False
    valid: bool = True
    state: ValidatorState = ValidatorState.WAITING
    
    # Timing
    token_count: int = 0
    char_count: int = 0
    tool_detected_at_token: int = 0
    tool_detected_at_char: int = 0
    tool_detected_at_time: float = 0.0
    first_error_at_token: int = 0
    first_error_at_time: float = 0.0
    complete_at_token: int = 0
    complete_at_time: float = 0.0
    
    # Timeline
    timeline: List[TimelineEvent] = dataclass_field(default_factory=list)
    
    @property
    def should_cancel(self) -> bool:
        """True if generation should be cancelled."""
        return bool(self.errors)
    
    @property
    def tool_allowed(self) -> bool:
        """True if detected tool is registered."""
        # Check if first error is UNKNOWN_TOOL
        if self.errors:
            return "unknown tool" not in self.errors[0].lower()
        return True


class StreamingValidator:
    """
    Validates GLYPH tool calls incrementally as tokens arrive.
    
    Usage:
        validator = StreamingValidator(registry)
        for token in llm_stream:
            result = validator.push_token(token)
            if result.should_cancel:
                cancel_generation()
                break
            if result.complete and result.valid:
                return result.fields
    """
    
    def __init__(self, registry: ToolRegistry):
        self.registry = registry
        self.reset()
    
    def reset(self):
        """Reset validator state for reuse."""
        self.buffer = ""
        self.state = ValidatorState.WAITING
        self.depth = 0
        self.in_string = False
        self.escape_next = False
        self.current_key = ""
        self.current_val = ""
        self.has_key = False
        self.tool_name = ""
        self.fields: Dict[str, Any] = {}
        self.errors: List[str] = []
        
        # Timing
        self.token_count = 0
        self.char_count = 0
        self.start_time = None
        self.tool_detected_at_token = 0
        self.tool_detected_at_char = 0
        self.tool_detected_at_time = 0.0
        self.first_error_at_token = 0
        self.first_error_at_time = 0.0
        self.complete_at_token = 0
        self.complete_at_time = 0.0
        self.timeline: List[TimelineEvent] = []
    
    def start(self):
        """Begin timing the validation session."""
        self.start_time = time.time()
    
    def push_token(self, token: str) -> StreamValidationResult:
        """
        Process a token from the LLM.
        
        Args:
            token: Token string from LLM
            
        Returns:
            StreamValidationResult with current state
        """
        if self.start_time is None:
            self.start()
        
        self.token_count += 1
        
        for char in token:
            self.char_count += 1
            self._process_char(char)
        
        return self._get_result()
    
    def _process_char(self, char: str):
        """Process a single character."""
        self.buffer += char
        
        # Handle escape sequences
        if self.escape_next:
            self.escape_next = False
            self.current_val += char
            return
        
        if char == '\\' and self.in_string:
            self.escape_next = True
            self.current_val += char
            return
        
        # Handle quotes
        if char == '"':
            if self.in_string:
                self.in_string = False
            else:
                self.in_string = True
                self.current_val = ""
            return
        
        # Inside string - accumulate
        if self.in_string:
            self.current_val += char
            return
        
        # Handle structural characters
        if char == '{':
            if self.state == ValidatorState.WAITING:
                # Capture tool name before entering object
                if self.current_val.strip():
                    self.tool_name = self.current_val.strip()
                    self.current_val = ""
                    # Check if tool is allowed
                    if not self.registry.is_allowed(self.tool_name):
                        self.errors.append(f"UNKNOWN_TOOL: {self.tool_name}")
                self.state = ValidatorState.IN_OBJECT
            self.depth += 1
        
        elif char == '}':
            self.depth -= 1
            if self.depth == 0:
                self._finish_field()
                self.state = ValidatorState.COMPLETE
                self._validate_complete()
        
        elif char == '[':
            self.depth += 1
            self.current_val += char
        
        elif char == ']':
            self.depth -= 1
            self.current_val += char
        
        elif char == '=':
            if self.depth == 1 and not self.has_key:
                self.current_key = self.current_val.strip()
                self.current_val = ""
                self.has_key = True
            else:
                self.current_val += char
        
        elif char in (' ', '\n', '\t', '\r'):
            if self.depth == 1 and self.has_key and self.current_val:
                self._finish_field()
        
        else:
            if self.state == ValidatorState.WAITING and self.depth == 0:
                # Accumulating tool name
                self.current_val += char
            elif self.depth == 1:
                self.current_val += char
    
    def _finish_field(self):
        """Finish parsing a field and add to fields dict."""
        if not self.has_key:
            return

        # Parse field value
        value = self._parse_value(self.current_val.strip())
        if self.current_key:
            self.fields[self.current_key] = value

            # Validate against schema if available
            if self.tool_name:
                tool_schema = self.registry.get_tool(self.tool_name)
                if tool_schema and self.current_key in tool_schema.args:
                    arg_schema = tool_schema.args[self.current_key]
                    error = arg_schema.validate(value)
                    if error and error not in self.errors:
                        self.errors.append(error)

        self.current_key = ""
        self.current_val = ""
        self.has_key = False
    
    def _parse_value(self, val_str: str) -> Any:
        """Parse a simple value string."""
        if not val_str:
            return None
        
        val_str = val_str.strip()
        
        # Null
        if val_str in ('_', '∅', 'null'):
            return None
        
        # Bool
        if val_str in ('t', 'true'):
            return True
        if val_str in ('f', 'false'):
            return False
        
        # Numbers
        try:
            if '.' in val_str or 'e' in val_str.lower():
                return float(val_str)
            return int(val_str)
        except ValueError:
            pass
        
        # String (quoted or bare)
        return val_str
    
    def _validate_complete(self):
        """Validate the complete tool call."""
        if not self.tool_name:
            self.errors.append("No tool name found")
            return
        
        # Check if tool is allowed
        if not self.registry.is_allowed(self.tool_name):
            if "UNKNOWN_TOOL" not in self.errors:
                self.errors.append(f"UNKNOWN_TOOL: {self.tool_name}")
            return
        
        # Validate against schema
        tool_schema = self.registry.get_tool(self.tool_name)
        if tool_schema:
            error = tool_schema.validate(self.fields)
            if error and error not in self.errors:
                self.errors.append(error)
    
    def _get_result(self) -> StreamValidationResult:
        """Get the current validation result."""
        elapsed = time.time() - self.start_time if self.start_time else 0.0

        # Early tool detection: if we're still waiting and have accumulated text
        effective_tool_name = self.tool_name
        if not effective_tool_name and self.state == ValidatorState.WAITING:
            candidate = self.current_val.strip()
            if candidate:
                effective_tool_name = candidate

        # Record tool detection
        if effective_tool_name and self.tool_detected_at_token == 0:
            self.tool_detected_at_token = self.token_count
            self.tool_detected_at_char = self.char_count
            self.tool_detected_at_time = elapsed
            allowed = self.registry.is_allowed(effective_tool_name)
            self.timeline.append(TimelineEvent(
                event="TOOL_DETECTED",
                token_count=self.token_count,
                char_count=self.char_count,
                elapsed=elapsed,
                detail=f"tool={effective_tool_name} allowed={allowed}"
            ))
        
        # Record first error
        if self.errors and self.first_error_at_token == 0:
            self.first_error_at_token = self.token_count
            self.first_error_at_time = elapsed
            self.timeline.append(TimelineEvent(
                event="ERROR",
                token_count=self.token_count,
                char_count=self.char_count,
                elapsed=elapsed,
                detail=self.errors[0]
            ))
        
        # Record completion
        if self.state == ValidatorState.COMPLETE and self.complete_at_token == 0:
            self.complete_at_token = self.token_count
            self.complete_at_time = elapsed
            valid = len(self.errors) == 0
            self.timeline.append(TimelineEvent(
                event="COMPLETE",
                token_count=self.token_count,
                char_count=self.char_count,
                elapsed=elapsed,
                detail=f"valid={valid}"
            ))
        
        return StreamValidationResult(
            tool_name=effective_tool_name,
            fields=self.fields.copy(),
            errors=self.errors.copy(),
            complete=self.state == ValidatorState.COMPLETE,
            valid=len(self.errors) == 0,
            state=self.state,
            token_count=self.token_count,
            char_count=self.char_count,
            tool_detected_at_token=self.tool_detected_at_token,
            tool_detected_at_char=self.tool_detected_at_char,
            tool_detected_at_time=self.tool_detected_at_time,
            first_error_at_token=self.first_error_at_token,
            first_error_at_time=self.first_error_at_time,
            complete_at_token=self.complete_at_token,
            complete_at_time=self.complete_at_time,
            timeline=self.timeline.copy(),
        )
    
    def get_result(self) -> StreamValidationResult:
        """Get the current validation result."""
        return self._get_result()
    
    def get_parsed(self) -> Optional[Dict[str, Any]]:
        """Get the parsed fields if validation is complete and valid."""
        if self.state == ValidatorState.COMPLETE and len(self.errors) == 0:
            return self.fields.copy()
        return None
