import logging
from secrets import token_hex
from time import strftime

from discord.ext import commands

from ..common.sqlhandle import SQLThread
from ..common.sqlutils import construct_schema, destroy_schema, snapshot_schema
from ..common.utils import sqlthread

discord_logger = logging.getLogger('discord')


class SQLUtils(commands.Cog):
    """A Cog that provides SQL utility methods for an Administrator."""
    def __init__(self, sqlthread: SQLThread):
        self.sql = sqlthread

    @commands.command(brief="Constructs the SQLite tables.")
    @commands.is_owner()
    async def construct(self, ctx: commands.Context):
        construct_schema(self.sql)
        await ctx.send("Finished constructing schema without issue.")

    @commands.command(brief="Destroys the SQLite tables.")
    @commands.is_owner()
    async def destroy(self, ctx: commands.Context):
        destroy_schema(self.sql)
        await ctx.send("Finished destroying schema without issue.")

    @commands.command(brief="Backs up the SQLite tables to a file.")
    @commands.is_owner()
    async def backup(self, ctx: commands.Context, filename: str = "backup"):
        filename += strftime("%Y%m%d%H%M%S") + "-" + token_hex(16) + ".db"
        await ctx.send("Saved to filename: {}".format(filename))
        snapshot_schema(self.sql, filename)
        await ctx.send("Finished snapshotting schema without issue.")


def setup(bot: commands.Bot):
    bot.add_cog(SQLUtils(sqlthread))
    discord_logger.info("Loaded extension discord.admin")


def teardown(bot: commands.Bot):
    bot.remove_cog("SQLUtils")
    discord_logger.info("Unloaded extension discord.admin")
