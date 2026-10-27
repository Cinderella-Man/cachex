defmodule Cachex.Actions.Warm do
  @moduledoc false
  # Command module to trigger manual cache warming.
  #
  # The only reason to call this command is the case in which you already
  # know the backing state of your cache has been updated and you need to
  # immediately refresh your warmed entries.
  import Cachex.Spec

  alias Cachex.Services

  ##############
  # Public API #
  ##############

  @doc """
  Triggers a manual warming in a cache.

  The warmers are fetched back out of the supervision tree, by calling out
  to our services module. This allows us to avoid having to track any special
  state in order to support manual warming.

  You can provide an `:only` option to restrict the warming to a specific set
  of warmer modules or names. The list can contain either the name of the
  module, or the name of the registered server. The previous `:modules` option
  is still accepted as an alias of `:only`.

  The result of this call contains a reference to every warmer which had a
  warming triggered, in the order the warmers were attached to the cache. A
  named warmer is referenced by its name, and an unnamed warmer by its module.
  """
  def execute(cache(warmers: warmers) = cache, options) do
    only = Keyword.get(options, :only, Keyword.get(options, :modules))
    wait = Keyword.get(options, :wait, false)

    warmed =
      warmers
      |> Enum.filter(&filter_mod(&1, only))
      |> Enum.map(&spawn_call(cache, &1, wait))
      |> Task.yield_many(:infinity)
      |> Enum.map(&extract_name/1)

    {:ok, warmed}
  end

  ###############
  # Private API #
  ###############

  # Filters warmers based on the :only flag for module/name.
  defp filter_mod(warmer(module: mod, name: name), only),
    do: only == nil or mod in only or name in only

  # Spawns a task to invoke the call to the remote warmer.
  defp spawn_call(cache, warmer(module: module, name: name) = warmer, wait) do
    Task.async(fn ->
      cache
      |> Services.warmer_target(warmer)
      |> call_warmer(wait)

      reference(module, name)
    end)
  end

  # Invokes a warmer with blocking enabled.
  defp call_warmer(target, true),
    do: GenServer.call(target, :cachex_warmer, :infinity)

  # Invokes a warmer with blocking disabled.
  defp call_warmer(target, _),
    do: send(target, :cachex_warmer)

  # Converts a task result to a name reference.
  defp extract_name({_, {:ok, name}}),
    do: name

  # Generates the reference reported for a warmer.
  #
  # Unnamed warmers are linked to their process identifier when the cache
  # starts, so they're reported by their module (as they were before warmers
  # could be named) rather than by an anonymous process identifier.
  defp reference(module, name) when is_pid(name),
    do: module

  defp reference(_module, name),
    do: name
end
