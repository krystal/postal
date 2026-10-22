# frozen_string_literal: true

require "rails_helper"

RSpec.describe "ServersController", type: :request do
  let(:organization) { create(:organization) }
  let(:server) { create(:server, organization: organization) }

  before do
    post "/login", params: { email_address: user.email_address, password: "passw0rd" }
  end

  describe "PATCH /org/:org/servers/:server" do
    context "when the user is an admin" do
      let(:user) { create(:user, admin: true) }

      it "updates the priority" do
        patch "/org/#{organization.permalink}/servers/#{server.permalink}", params: { server: { priority: 100 } }
        expect(response).to redirect_to(organization_server_path(organization, server))
        expect(server.reload.priority).to eq 100
      end
    end

    context "when the user is not an admin" do
      let(:user) { create(:user) }

      before do
        OrganizationUser.create!(organization: organization, user: user, admin: true, all_servers: true)
      end

      it "updates other settings but not the priority" do
        patch "/org/#{organization.permalink}/servers/#{server.permalink}", params: { server: { name: "Renamed Server", priority: 100 } }
        expect(response).to redirect_to(organization_server_path(organization, server))
        server.reload
        expect(server.name).to eq "Renamed Server"
        expect(server.priority).to eq 50
      end
    end
  end
end
