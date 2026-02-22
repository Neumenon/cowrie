"""
GLYPH Parser

Parses GLYPH-Loose text format into GValue objects.
"""

from __future__ import annotations
import re
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Any

from .types import GValue, GType, MapEntry, RefID


# ============================================================
# Lexer
# ============================================================

class TokenType:
    EOF = "EOF"
    LBRACE = "{"
    RBRACE = "}"
    LBRACKET = "["
    RBRACKET = "]"
    LPAREN = "("
    RPAREN = ")"
    EQUALS = "="
    COLON = ":"
    COMMA = ","
    PIPE = "|"
    CARET = "^"
    AT = "@"
    NULL = "NULL"
    BOOL = "BOOL"
    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BYTES = "BYTES"
    IDENT = "IDENT"
    NEWLINE = "NEWLINE"


@dataclass
class Token:
    type: str
    value: Any
    pos: int


class Lexer:
    """Tokenizer for GLYPH text."""

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.length = len(text)

    def peek_char(self) -> str:
        if self.pos >= self.length:
            return ""
        return self.text[self.pos]

    def next_char(self) -> str:
        if self.pos >= self.length:
            return ""
        c = self.text[self.pos]
        self.pos += 1
        return c

    def skip_whitespace(self) -> None:
        while self.pos < self.length and self.text[self.pos] in " \t\r":
            self.pos += 1

    def skip_whitespace_and_newlines(self) -> None:
        while self.pos < self.length and self.text[self.pos] in " \t\r\n":
            self.pos += 1

    def next_token(self) -> Token:
        self.skip_whitespace()

        if self.pos >= self.length:
            return Token(TokenType.EOF, None, self.pos)

        start = self.pos
        c = self.peek_char()

        # Single character tokens
        if c == "{":
            self.pos += 1
            return Token(TokenType.LBRACE, c, start)
        if c == "}":
            self.pos += 1
            return Token(TokenType.RBRACE, c, start)
        if c == "[":
            self.pos += 1
            return Token(TokenType.LBRACKET, c, start)
        if c == "]":
            self.pos += 1
            return Token(TokenType.RBRACKET, c, start)
        if c == "(":
            self.pos += 1
            return Token(TokenType.LPAREN, c, start)
        if c == ")":
            self.pos += 1
            return Token(TokenType.RPAREN, c, start)
        if c == "=":
            self.pos += 1
            return Token(TokenType.EQUALS, c, start)
        if c == ":":
            self.pos += 1
            return Token(TokenType.COLON, c, start)
        if c == ",":
            self.pos += 1
            return Token(TokenType.COMMA, c, start)
        if c == "|":
            self.pos += 1
            return Token(TokenType.PIPE, c, start)
        if c == "^":
            self.pos += 1
            return Token(TokenType.CARET, c, start)
        if c == "@":
            self.pos += 1
            return Token(TokenType.AT, c, start)
        if c == "\n":
            self.pos += 1
            return Token(TokenType.NEWLINE, c, start)

        # Null symbol
        if c == "∅" or c == "_":
            self.pos += 1
            return Token(TokenType.NULL, None, start)

        # Quoted string
        if c == '"':
            return self._read_string()

        # Bytes literal
        if c == "b" and self.pos + 1 < self.length:
            if self.text[self.pos:self.pos + 4] == 'b64"':
                return self._read_bytes()

        # Number or identifier
        if c == "-" or c.isdigit():
            return self._read_number_or_ident()

        # Identifier or keyword
        if c.isalpha() or c == "_":
            return self._read_ident()

        raise ValueError(f"unexpected character '{c}' at position {self.pos}")

    def _read_string(self) -> Token:
        start = self.pos
        self.pos += 1  # Skip opening quote
        result = []

        while self.pos < self.length:
            c = self.text[self.pos]
            if c == '"':
                self.pos += 1
                return Token(TokenType.STRING, "".join(result), start)
            if c == '\\':
                self.pos += 1
                if self.pos >= self.length:
                    raise ValueError("unterminated escape sequence")
                esc = self.text[self.pos]
                if esc == 'n':
                    result.append('\n')
                elif esc == 'r':
                    result.append('\r')
                elif esc == 't':
                    result.append('\t')
                elif esc == '"':
                    result.append('"')
                elif esc == '\\':
                    result.append('\\')
                elif esc == 'u':
                    if self.pos + 5 > self.length:
                        raise ValueError("invalid unicode escape")
                    hex_str = self.text[self.pos + 1:self.pos + 5]
                    result.append(chr(int(hex_str, 16)))
                    self.pos += 4
                else:
                    result.append(esc)
            else:
                result.append(c)
            self.pos += 1

        raise ValueError("unterminated string")

    def _read_bytes(self) -> Token:
        start = self.pos
        self.pos += 4  # Skip b64"
        result = []

        while self.pos < self.length:
            c = self.text[self.pos]
            if c == '"':
                self.pos += 1
                b64_str = "".join(result)
                try:
                    data = base64.b64decode(b64_str)
                except Exception as e:
                    raise ValueError(f"invalid base64: {e}")
                return Token(TokenType.BYTES, data, start)
            result.append(c)
            self.pos += 1

        raise ValueError("unterminated bytes literal")

    def _read_number_or_ident(self) -> Token:
        start = self.pos
        result = []

        # Could be negative number or identifier starting with -
        if self.peek_char() == '-':
            result.append(self.next_char())

        # Read digits and decimal point
        has_dot = False
        has_exp = False

        while self.pos < self.length:
            c = self.peek_char()
            if c.isdigit():
                result.append(self.next_char())
            elif c == '.' and not has_dot and not has_exp:
                has_dot = True
                result.append(self.next_char())
            elif c in 'eE' and not has_exp:
                has_exp = True
                result.append(self.next_char())
                if self.peek_char() in '+-':
                    result.append(self.next_char())
            elif c.isalpha() or c == '_':
                # It's an identifier
                while self.pos < self.length and (self.peek_char().isalnum() or self.peek_char() in '_-./@+'):
                    result.append(self.next_char())
                return Token(TokenType.IDENT, "".join(result), start)
            else:
                break

        s = "".join(result)

        # Determine if it's int or float
        if has_dot or has_exp:
            return Token(TokenType.FLOAT, float(s), start)
        else:
            try:
                return Token(TokenType.INT, int(s), start)
            except ValueError:
                return Token(TokenType.IDENT, s, start)

    def _read_ident(self) -> Token:
        start = self.pos
        result = []

        while self.pos < self.length:
            c = self.peek_char()
            if c.isalnum() or c in '_-./@+':
                result.append(self.next_char())
            else:
                break

        s = "".join(result)

        # Check for keywords
        if s == "t" or s == "true":
            return Token(TokenType.BOOL, True, start)
        if s == "f" or s == "false":
            return Token(TokenType.BOOL, False, start)
        if s == "null" or s == "nil":
            return Token(TokenType.NULL, None, start)

        return Token(TokenType.IDENT, s, start)


# ============================================================
# Parser
# ============================================================

class Parser:
    """Recursive descent parser for GLYPH text."""

    def __init__(self, text: str):
        self.lexer = Lexer(text)
        self.peeked: Optional[Token] = None
        # Don't read initial token here - let parse() do it

    def peek(self) -> Token:
        if self.peeked is None:
            self.peeked = self.lexer.next_token()
        return self.peeked

    def advance(self) -> Token:
        if self.peeked is not None:
            self.current = self.peeked
            self.peeked = None
        else:
            self.current = self.lexer.next_token()
        return self.current

    def expect(self, token_type: str) -> Token:
        if self.current.type != token_type:
            raise ValueError(f"expected {token_type}, got {self.current.type}")
        tok = self.current
        self.advance()
        return tok

    def parse(self) -> GValue:
        """Parse the input and return a GValue."""
        self.lexer.skip_whitespace_and_newlines()
        self.current = self.lexer.next_token()
        v = self._parse_value()
        return v

    def _parse_value(self) -> GValue:
        """Parse a single value."""
        tok = self.current

        if tok.type == TokenType.NULL:
            self.advance()
            return GValue.null()

        if tok.type == TokenType.BOOL:
            self.advance()
            return GValue.bool_(tok.value)

        if tok.type == TokenType.INT:
            self.advance()
            return GValue.int_(tok.value)

        if tok.type == TokenType.FLOAT:
            self.advance()
            return GValue.float_(tok.value)

        if tok.type == TokenType.STRING:
            self.advance()
            return GValue.str_(tok.value)

        if tok.type == TokenType.BYTES:
            self.advance()
            return GValue.bytes_(tok.value)

        if tok.type == TokenType.CARET:
            return self._parse_ref()

        if tok.type == TokenType.LBRACKET:
            return self._parse_list()

        if tok.type == TokenType.LBRACE:
            return self._parse_map()

        if tok.type == TokenType.AT:
            return self._parse_directive()

        if tok.type == TokenType.IDENT:
            # Could be: bare string, struct, or sum
            return self._parse_ident_value()

        raise ValueError(f"unexpected token {tok.type} at position {tok.pos}")

    def _parse_ref(self) -> GValue:
        """Parse a reference (^prefix:value or ^value)."""
        self.expect(TokenType.CARET)

        if self.current.type == TokenType.STRING:
            # Quoted reference
            s = self.current.value
            self.advance()
            if ':' in s:
                prefix, value = s.split(':', 1)
                return GValue.id(prefix, value)
            return GValue.id("", s)

        # Get first part (could be IDENT, BOOL, INT - we treat them all as text)
        if self.current.type == TokenType.IDENT:
            first = self.current.value
            self.advance()
        elif self.current.type == TokenType.BOOL:
            # t/f as identifiers in ref context
            first = "t" if self.current.value else "f"
            self.advance()
        elif self.current.type == TokenType.INT:
            first = str(self.current.value)
            self.advance()
        else:
            raise ValueError(f"expected reference value, got {self.current.type}")

        if self.current.type == TokenType.COLON:
            self.advance()
            # Second part
            if self.current.type == TokenType.IDENT:
                second = self.current.value
                self.advance()
            elif self.current.type == TokenType.STRING:
                second = self.current.value
                self.advance()
            elif self.current.type == TokenType.INT:
                second = str(self.current.value)
                self.advance()
            elif self.current.type == TokenType.BOOL:
                second = "t" if self.current.value else "f"
                self.advance()
            else:
                raise ValueError(f"expected reference value part, got {self.current.type}")
            return GValue.id(first, second)

        return GValue.id("", first)

    def _parse_list(self) -> GValue:
        """Parse a list [...] """
        self.expect(TokenType.LBRACKET)
        items = []

        while self.current.type != TokenType.RBRACKET:
            if self.current.type == TokenType.EOF:
                raise ValueError("unterminated list")
            if self.current.type == TokenType.COMMA:
                self.advance()
                continue
            if self.current.type == TokenType.NEWLINE:
                self.advance()
                continue

            items.append(self._parse_value())

        self.expect(TokenType.RBRACKET)
        return GValue.list_(*items)

    def _parse_map(self) -> GValue:
        """Parse a map {...}"""
        self.expect(TokenType.LBRACE)
        entries = []

        while self.current.type != TokenType.RBRACE:
            if self.current.type == TokenType.EOF:
                raise ValueError("unterminated map")
            if self.current.type == TokenType.COMMA:
                self.advance()
                continue
            if self.current.type == TokenType.NEWLINE:
                self.advance()
                continue

            # Parse key
            if self.current.type == TokenType.IDENT:
                key = self.current.value
                self.advance()
            elif self.current.type == TokenType.STRING:
                key = self.current.value
                self.advance()
            else:
                raise ValueError(f"expected key, got {self.current.type}")

            # Expect = or :
            if self.current.type in (TokenType.EQUALS, TokenType.COLON):
                self.advance()

            # Parse value
            value = self._parse_value()
            entries.append(MapEntry(key, value))

        self.expect(TokenType.RBRACE)
        return GValue.map_(*entries)

    def _parse_ident_value(self) -> GValue:
        """Parse an identifier which could be a bare string, struct, or sum."""
        name = self.current.value
        self.advance()

        if self.current.type == TokenType.LBRACE:
            # Struct: Name{...}
            self.advance()
            fields = []

            while self.current.type != TokenType.RBRACE:
                if self.current.type == TokenType.EOF:
                    raise ValueError("unterminated struct")
                if self.current.type == TokenType.COMMA:
                    self.advance()
                    continue
                if self.current.type == TokenType.NEWLINE:
                    self.advance()
                    continue

                # Parse field
                if self.current.type == TokenType.IDENT:
                    key = self.current.value
                    self.advance()
                elif self.current.type == TokenType.STRING:
                    key = self.current.value
                    self.advance()
                else:
                    raise ValueError(f"expected field name, got {self.current.type}")

                if self.current.type in (TokenType.EQUALS, TokenType.COLON):
                    self.advance()

                value = self._parse_value()
                fields.append(MapEntry(key, value))

            self.expect(TokenType.RBRACE)
            return GValue.struct(name, *fields)

        if self.current.type == TokenType.LPAREN:
            # Sum: Tag(value) or Tag()
            self.advance()

            if self.current.type == TokenType.RPAREN:
                self.advance()
                return GValue.sum(name, None)

            value = self._parse_value()
            self.expect(TokenType.RPAREN)
            return GValue.sum(name, value)

        # Bare string
        return GValue.str_(name)

    def _parse_directive(self) -> GValue:
        """Parse a directive like @tab or @schema."""
        self.expect(TokenType.AT)

        if self.current.type != TokenType.IDENT:
            raise ValueError(f"expected directive name, got {self.current.type}")

        directive = self.current.value
        self.advance()

        if directive == "tab":
            return self._parse_tabular()

        raise ValueError(f"unknown directive: {directive}")

    def _parse_tabular(self) -> GValue:
        """Parse tabular format: @tab _ [cols] |row|... @end"""
        # Skip the _ placeholder
        if self.current.type == TokenType.IDENT and self.current.value == "_":
            self.advance()
        elif self.current.type == TokenType.NULL:
            self.advance()

        # Parse column headers
        if self.current.type != TokenType.LBRACKET:
            raise ValueError("expected [ for column headers")

        self.advance()
        cols = []
        while self.current.type != TokenType.RBRACKET:
            if self.current.type == TokenType.IDENT:
                cols.append(self.current.value)
                self.advance()
            elif self.current.type == TokenType.STRING:
                cols.append(self.current.value)
                self.advance()
            elif self.current.type == TokenType.COMMA:
                self.advance()
            elif self.current.type == TokenType.NEWLINE:
                self.advance()
            else:
                raise ValueError(f"expected column name, got {self.current.type}")

        self.expect(TokenType.RBRACKET)

        # Parse rows
        rows = []
        while True:
            # Skip newlines
            while self.current.type == TokenType.NEWLINE:
                self.advance()

            if self.current.type == TokenType.AT:
                self.advance()
                if self.current.type == TokenType.IDENT and self.current.value == "end":
                    self.advance()
                    break
                raise ValueError("expected @end")

            if self.current.type == TokenType.PIPE:
                row = self._parse_tabular_row(cols)
                rows.append(row)
            elif self.current.type == TokenType.EOF:
                break
            else:
                raise ValueError(f"expected row or @end, got {self.current.type}")

        return GValue.list_(*rows)

    def _parse_tabular_row(self, cols: List[str]) -> GValue:
        """Parse a single tabular row: |val|val|val|"""
        # The current token is PIPE. The lexer.pos is right after the pipe character.
        # We need to read raw characters for each cell, not tokenize them.
        if self.current.type != TokenType.PIPE:
            raise ValueError(f"expected |, got {self.current.type}")

        # lexer.pos is right after the opening pipe - that's where cell content starts
        entries = []

        for i, col in enumerate(cols):
            # Read until next pipe (handling escapes)
            cell_chars = []

            while self.lexer.pos < self.lexer.length:
                c = self.lexer.text[self.lexer.pos]
                if c == '|':
                    break
                if c == '\\' and self.lexer.pos + 1 < self.lexer.length:
                    next_c = self.lexer.text[self.lexer.pos + 1]
                    if next_c == '|':
                        cell_chars.append('|')
                        self.lexer.pos += 2
                        continue
                    elif next_c == 'n':
                        cell_chars.append('\n')
                        self.lexer.pos += 2
                        continue
                    elif next_c == '\\':
                        cell_chars.append('\\')
                        self.lexer.pos += 2
                        continue
                cell_chars.append(c)
                self.lexer.pos += 1

            # Must have a pipe after cell
            if self.lexer.pos >= self.lexer.length or self.lexer.text[self.lexer.pos] != '|':
                raise ValueError("expected | after cell")
            self.lexer.pos += 1  # Skip the closing pipe

            cell_text = "".join(cell_chars).strip()

            # Parse cell value
            if cell_text == "" or cell_text == "∅" or cell_text == "_":
                value = GValue.null()
            else:
                cell_parser = Parser(cell_text)
                value = cell_parser.parse()

            entries.append(MapEntry(col, value))

        # Now get the next token (should be NEWLINE or @)
        self.current = self.lexer.next_token()

        return GValue.map_(*entries)


# ============================================================
# Public API
# ============================================================

def parse_loose(text: str) -> GValue:
    """Parse GLYPH-Loose text into a GValue."""
    parser = Parser(text)
    return parser.parse()


def parse(text: str) -> GValue:
    """Alias for parse_loose."""
    return parse_loose(text)
