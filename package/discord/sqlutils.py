import logging
from secrets import token_hex
from time import strftime, gmtime
import sqlite3

import discord
from discord.ext import commands

from ..common.sqlhandle import SQLThread
from ..common.sqlutils import Status, construct_schema, destroy_schema, snapshot_schema, make_request, get_status
from ..common.utils import sqlthread, name_string

discord_logger = logging.getLogger('discord')


class Database(commands.Cog):
    """A Cog that provides SQL utility methods for an Administrator."""
    def __init__(self, bot: commands.Bot, sql: SQLThread):
        commands.Cog.__init__(self)
        self.sql = sql
        self.bot = bot

    @commands.command(brief="Constructs the SQLite tables.", help="Constructs the SQLite schema. No arguments.")
    @commands.is_owner()
    async def construct(self, ctx: commands.Context):
        res = construct_schema(self.sql)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished constructing schema without issue.")

    @commands.command(brief="Destroys the SQLite tables.", help="Destroys the SQLite schema.")
    @commands.is_owner()
    async def destroy(self, ctx: commands.Context):
        res = destroy_schema(self.sql)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished destroying schema without issue.")

    @commands.command(brief="Backs up the SQLite tables to a file.", help="Takes argument filename. Automatically " +
        "appends datetime data and file extension, with token to prevent collisions.")
    @commands.is_owner()
    async def backup(self, ctx: commands.Context, filename: str = "backup"):
        filename += strftime("%Y-%m-%d-%H-%M-%S") + "-" + token_hex(16) + ".sqlite"
        filename = "backups/" + filename
        await ctx.send("Saved to filename: {}".format(filename))
        res = snapshot_schema(self.sql, filename)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Finished snapshotting schema without issue.")

    @commands.command(brief="Make a SQL request and get a result (if any).", help="Greedily takes string for request.")
    @commands.is_owner()
    async def request(self, ctx: commands.Context, *, request: str):
        res = make_request(self.sql, request)
        if isinstance(res, sqlite3.Error):
            raise res
        await ctx.send("Result: ``" + str(res) + "``")

    @commands.command(brief="Get the current status.")
    async def status(self, ctx: commands.Context):
        status = list(get_status(self.sql))[0]
        embed = discord.Embed(color=0x3daeff)
        embed.title = "Current mTWOW Status"
        embed.set_author(name=self.bot.user.name) \
            .add_field(name="Round Number", value=str(status.round_num)) \
            .add_field(name="Prompt", value=status.prompt) \
            .add_field(name="Phase", value=status.phase) \
            .add_field(name="Deadline of Current Phase", value=strftime("%a, %d %b %Y %H:%M:%S +0000",
                                                                         gmtime(status.deadline / 1000))) \
            .set_footer(text=name_string(self.bot.get_user(self.bot.owner_id)))
        await ctx.send(embed=embed)

def setup(bot: commands.Bot):
    bot.add_cog(Database(bot, sqlthread))
    discord_logger.info("Loaded extension discord.sqlutils")


def teardown(bot: commands.Bot):
    bot.remove_cog("Database")
    discord_logger.info("Unloaded extension discord.sqlutils")