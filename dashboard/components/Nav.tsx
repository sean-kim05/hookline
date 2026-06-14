"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Deliveries" },
  { href: "/dlq", label: "Dead-letter queue" },
  { href: "/endpoints", label: "Endpoints" },
];

export function Nav() {
  const path = usePathname();
  return (
    <nav className="nav">
      <span className="brand">⎈ Hookline</span>
      {links.map((l) => (
        <Link key={l.href} href={l.href} className={path === l.href ? "active" : ""}>
          {l.label}
        </Link>
      ))}
    </nav>
  );
}
