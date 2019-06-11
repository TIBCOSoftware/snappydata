# Detecting and Handling Network Segmentation ("Split Brain")

When network segmentation occurs, a distributed system that does not handle the partition condition properly allows multiple subgroups to form. This condition can lead to numerous problems, including distributed applications operating on inconsistent data.

For example, because thin clients connecting to a server cluster are not tied into the membership system, a client might communicate with servers from multiple subgroups. Or, one set of clients might see one subgroup of servers while another set of clients cannot see that subgroup but can see another one.

SnappyData handles this problem by allowing only one subgroup to form and survive. The distributed systems and caches of other subgroups are shut down as quickly as possible. Appropriate alerts are raised through the SnappyData logging system to alert administrators to take action.

Network partition detection in SnappyData is based on the concept of a lead member and a group management coordinator. The coordinator is a member that manages entry and exit of other members of the distributed system. For network partition detection, the coordinator is always a SnappyData locator. The lead member is always the oldest member of the distributed system that does not have a locator running in the same process. Given this, two situations causes SnappyData to declare a network partition:

*   If both a locator and the lead member leave the distributed system abnormally within a configurable period of time, a network partition is declared and the caches of members who are unable to see the locator and the lead member are immediately closed and disconnected.

    If a locator or lead member's distributed system is shut down normally, SnappyData automatically elects a new one and continues to operate.

*   If no locator can be contacted by a member, it declares a network partition has occurred, closes itself, and disconnects from the distributed system.

You enable network partition detection by setting the [enable-network-partition-detection](/reference/configuration_parameters/enable-network-partition-detection.md) distributed system property to **true**. Enable network partition detection in all locators and in any other process that should be sensitive to network partitioning. Processes that do not have network partition detection enabled are not eligible to be the lead member, so their failure does not trigger declaration of a network partition.

<note-sub>
!!! Note
	The distributed system must contain locators to enable network partition detection.
</note-sub>

