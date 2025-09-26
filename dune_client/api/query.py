"""
CRUD API endpoints enables users to
create, read, update, make public/private or archive queries beyond the Dune IDE.
Enables more flexible integration of Dune API into your workflow
and freeing you from UI-exclusive query editing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dune_client.api.base import BaseRouter
from dune_client.models import DuneError
from dune_client.query import DuneQuery

if TYPE_CHECKING:
    from dune_client.types import QueryParameter


class QueryAPI(BaseRouter):
    """
    Implementation of Query API (aka CRUD) Operations - Plus subscription only
    https://docs.dune.com/api-reference/queries/endpoint/query-object
    """

    def create_query(
        self,
        name: str,
        query_sql: str,
        params: list[QueryParameter] | None = None,
        is_private: bool = False,
    ) -> DuneQuery:
        """
        Creates Dune Query by ID
        https://docs.dune.com/api-reference/queries/endpoint/create
        """
        payload = {
            "name": name,
            "query_sql": query_sql,
            "is_private": is_private,
        }
        if params is not None:
            payload["parameters"] = [p.to_dict() for p in params]
        response_json = self._post(route="/query/", params=payload)
        try:
            query_id = int(response_json["query_id"])
            # Note that this requires an extra request.
            return self.get_query(query_id)
        except KeyError as err:
            raise DuneError(response_json, "CreateQueryResponse", err) from err

    def get_query(self, query_id: int) -> DuneQuery:
        """
        Retrieves Dune Query by ID
        https://docs.dune.com/api-reference/queries/endpoint/read
        """
        response_json = self._get(route=f"/query/{query_id}")
        return DuneQuery.from_dict(response_json)

    def update_query(
        self,
        query_id: int,
        name: str | None = None,
        query_sql: str | None = None,
        params: list[QueryParameter] | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """
        Updates Dune Query by ID
        https://docs.dune.com/api-reference/queries/endpoint/update

        The request body should contain all fields that need to be updated.
        Any omitted fields will be left untouched.
        If the tags or parameters are provided as an empty array,
        they will be deleted from the query.
        """
        parameters: dict[str, Any] = {}
        if name is not None:
            parameters["name"] = name
        if description is not None:
            parameters["description"] = description
        if tags is not None:
            parameters["tags"] = tags
        if query_sql is not None:
            parameters["query_sql"] = query_sql
        if params is not None:
            parameters["parameters"] = [p.to_dict() for p in params]

        if not bool(parameters):
            # Nothing to change no need to make reqeust
            self.logger.warning("called update_query with no proposed changes.")
            return query_id

        response_json = self._patch(
            route=f"/query/{query_id}",
            params=parameters,
        )
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return int(response_json["query_id"])
        except KeyError as err:
            raise DuneError(response_json, "UpdateQueryResponse", err) from err

    def archive_query(self, query_id: int) -> bool:
        """
        https://docs.dune.com/api-reference/queries/endpoint/archive
        returns resulting value of Query.is_archived
        """
        response_json = self._post(route=f"/query/{query_id}/archive")
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return self.get_query(int(response_json["query_id"])).meta.is_archived
        except KeyError as err:
            raise DuneError(response_json, "ArchiveQueryResponse", err) from err

    def unarchive_query(self, query_id: int) -> bool:
        """
        https://docs.dune.com/api-reference/queries/endpoint/unarchive
        returns resulting value of Query.is_archived
        """
        response_json = self._post(route=f"/query/{query_id}/unarchive")
        try:
            # No need to make a dataclass for this since it's just a boolean.
            return self.get_query(int(response_json["query_id"])).meta.is_archived
        except KeyError as err:
            raise DuneError(response_json, "UnarchiveQueryResponse", err) from err

    def make_private(self, query_id: int) -> None:
        """
        https://docs.dune.com/api-reference/queries/endpoint/private
        """
        response_json = self._post(route=f"/query/{query_id}/private")
        try:
            assert self.get_query(int(response_json["query_id"])).meta.is_private
        except KeyError as err:
            raise DuneError(response_json, "MakePrivateResponse", err) from err

    def make_public(self, query_id: int) -> None:
        """
        https://docs.dune.com/api-reference/queries/endpoint/unprivate
        """
        response_json = self._post(route=f"/query/{query_id}/unprivate")
        try:
            assert not self.get_query(int(response_json["query_id"])).meta.is_private
        except KeyError as err:
            raise DuneError(response_json, "MakePublicResponse", err) from err
