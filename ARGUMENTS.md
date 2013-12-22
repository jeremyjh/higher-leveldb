So, I'm rather new to Haskell and some of the design decisions I've made may be mistaken. This file exists for the purpose of allowing me to explain
my reasoning with regard to decisions I suspect would be controversial. It may also help remind me why I did some things so I don't go through the revert-rediscover process again.

With 0.2.0 I changed the MonadLevelDB class substantially. Specifically, I put primitive operations put/delete/get into the class. This is not really needed
begause these could already be implemented in terms of the existing class (e.g. using liftLevelDB). The entire reason for doing it was so that in a runBatch, these same
functions would be used but with different semantics: put/delete now add operations to a Writer list rather than executing directly against the database. I worry about this because
such a change in semantics should probably be more explicit. The main reason I have kept it, is because it allows one to compose the same MonadLevelDB functions within a runBatch as
within a 'normal' block. Consider this:

	```haskell
		addCheeseBurger  :: MonadLevelDB m => Key -> Value	-> m ()
	  addCheeseBurger k = withKeySpace "menu:burgers" $ put k

		delCheeseBurger :: MonadLevelDB m => Key -> m ()
	  delCheeseBurger = withKeySpace "menu:burgers" delete

		-- I know its a bad example as 'put' is already an 'upsert'
	  swapBurger :: MonadLevelDB m => Key -> Value -> m ()
	  swapBurger k v = runBatch $ do
			  deleteCheeseBurger k
        addCheeseBurger k v
  ```

So the add/del functions may exist just to fix the KeySpace - in a real system probably the values would be some sort of data structure you'd serialize first and
so these functions would also fix the type and do the serialization. The point here is that now I can compose these functions either in a runBatch - in which case
they add the BatchOps to the list and are all executed atomically - or outside of a batch in which case they are executed individually. Without the "classy" interface,
these functions would have to be duplicated for both batch (addCheeseBurgerB) and singleton (addCheeseBurger) commits if both usages are required.
