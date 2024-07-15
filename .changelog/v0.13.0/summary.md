<!--
    Add a summary for the release here.

    If you don't change this message, or if this file is empty, the release
    will not be created. -->
This release changes the contract of the Iterator Key() and Value() APIs.
Namely, the caller is now responsible for creating a copy of their returned value if they want to modify it.
