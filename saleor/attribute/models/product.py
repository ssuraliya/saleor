from django.db import models

from ...core.models import SortableModel
from ...product.models import Product, ProductType
from .base import AssociatedAttributeManager


class AssignedProductAttributeValue(SortableModel):
    value = models.ForeignKey(
        "AttributeValue",
        on_delete=models.CASCADE,
        related_name="productvalueassignment",
    )
    new_product = models.ForeignKey(
        Product,
        null=True,
        related_name="attributevalues",
        on_delete=models.CASCADE,
    )

    class Meta:
        unique_together = (("value", "new_product"),)
        ordering = ("sort_order", "pk")

    def get_ordering_queryset(self):
        return self.new_product.attributevalues.all()


class AttributeProduct(SortableModel):
    attribute = models.ForeignKey(
        "Attribute", related_name="attributeproduct", on_delete=models.CASCADE
    )
    product_type = models.ForeignKey(
        ProductType, related_name="attributeproduct", on_delete=models.CASCADE
    )

    objects = AssociatedAttributeManager()

    class Meta:
        unique_together = (("attribute", "product_type"),)
        ordering = ("sort_order", "pk")

    def get_ordering_queryset(self):
        return self.product_type.attributeproduct.all()
