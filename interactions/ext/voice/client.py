from interactions.base import get_logger
from interactions.client.bot import Client
from interactions.api.models import Snowflake

from .websocket import VoiceWebSocketClient
from typing import Union

__all__ = "VoiceClient"

log = get_logger("client")


class VoiceClient(Client):
    def __init__(self, token: str, **kwargs) -> None:
        super().__init__(token, **kwargs)
        self._websocket = VoiceWebSocketClient(token, self._intents, me=self.me)

    async def connect_vc(
        self,
        channel_id: Union[Snowflake, int],
        guild_id: Union[Snowflake, int],
        self_deaf: bool = False,
        self_mute: bool = False,
    ) -> None:
        """
        Connects the bot to a voice channel.
        :param channel_id: The id of the channel to connect to
        :type channel_id: Union[Snowflake, int]
        :param guild_id: The id of the guild the channel belongs to
        :type guild_id: Union[Snowflake, int]
        :param self_deaf: whether the bot is self-deafened
        :type self_deaf: bool
        :param self_mute: whether the bot is self-muted
        :type self_mute: bool
        """

        guild_id: str = str(guild_id)
        channel_id: str = str(channel_id)

        if guild_id in self._websocket._voice_connections.keys():
            if self._websocket._voice_connections[guild_id]._closed is True:
                del self._websocket._voice_connections[guild_id]

            else:
                log.warning(
                    "Already connected to a voice channel! Disconnect before creating a new connection!"
                )
                return

        await self._websocket._connect_vc(
            guild_id=guild_id,
            channel_id=channel_id,
            self_mute=self_mute,
            self_deaf=self_deaf,
        )

    async def play(self, guild_id: Union[Snowflake, int]) -> None:
        """
        Plays the audio stream.
        :param guild_id: The id of the guild to play the audio stream in
        :type guild_id: Union[Snowflake, int]
        """

        guild_id: str = str(guild_id)

        if guild_id not in self._websocket._voice_connections.keys():
            log.warning("Not connected to a voice channel!")
            return

        return await self._websocket._voice_connections[guild_id]._start_speaking()

    async def disconnect_vc(
        self,
        guild_id: Union[Snowflake, int],
    ) -> None:
        """
        Removes the bot of the channel.
        :param guild_id: The id of the guild to disconnect the bot from
        :type guild_id: Union[Snowflake, int]
        """

        guild_id: int = str(guild_id)

        if guild_id not in self._websocket._voice_connections.keys():
            log.warning("Not connected to a voice channel!")
            return

        return await self._websocket._disconnect_vc(guild_id=guild_id)

    async def disconnect_all_vc(self) -> None:
        """
        Disconnects all voice connections.
        """

        return await self._websocket._disconnect_all_vc()
